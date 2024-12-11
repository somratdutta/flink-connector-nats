// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.io.synadia.flink.v0;

import io.nats.client.*;
import io.nats.client.api.*;
import io.synadia.flink.v0.NatsJetStreamSource;
import io.synadia.flink.v0.NatsJetStreamSourceBuilder;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.payload.StringPayloadDeserializer;
import io.synadia.flink.v0.sink.NatsSink;
import io.synadia.io.synadia.flink.TestBase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;

import static io.nats.client.api.ConsumerConfiguration.INTEGER_UNSET;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class JsSourceTests extends TestBase {

    static void publish(JetStream js, String subject, int count) throws Exception {
        publish(js, subject, count, 0);
    }

    static void publish(JetStream js, String subject, int count, long delay) throws Exception {
        for (int x = 0; x < count; x++) {
            js.publish(subject, ("data-" + subject + "-" + x + "-" + random()).getBytes());
            if (delay > 0) {
                sleep(delay);
            }
        }
    }

    @Test
    public void testJsSourceBounded2() throws Exception {
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());
        String sourceSubject = random("sub");
        String sinkSubject = random("sink");
        String streamName = random("strm");
        String consumerName = random("con");

        runInExternalServer(true, (nc, url) -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = jsm.jetStream();

            createStream(jsm, streamName, sourceSubject);
            publish(js, sourceSubject, 10);

            ConsumerConfiguration cc = createConsumer(jsm, streamName, sourceSubject, consumerName, INTEGER_UNSET);

            // --------------------------------------------------------------------------------
            Properties connectionProperties = defaultConnectionProperties("tls://platform-nats-internal-cluster.nxengg.cloud:443");
            // Add TLS Configuration
            connectionProperties.put(Options.PROP_TRUSTSTORE, "/Users/somratdutta/PycharmProjects/sc-nats-flink-integration/creds/truststore.jks");
            connectionProperties.put(Options.PROP_TRUSTSTORE_PASSWORD, "changeit");
            connectionProperties.put(Options.PROP_CREDENTIAL_PATH, "/Users/somratdutta/PycharmProjects/sc-nats-flink-integration/creds/nats-stage-stream.creds");


            PayloadDeserializer<String> deserializer = new StringPayloadDeserializer();
            NatsJetStreamSourceBuilder<String> builder =
                new NatsJetStreamSourceBuilder<String>()
                    .subjects(sourceSubject)
                    .payloadDeserializer(deserializer)
                    .connectionProperties(connectionProperties)
                    .consumerName(consumerName);

            NatsJetStreamSource<String> natsSource = builder.build();
            StreamExecutionEnvironment env = getStreamExecutionEnvironment();
            env.getCheckpointConfig().setCheckpointInterval(10_000L);
            DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");

            // listen to the sink output
            Dispatcher d = nc.createDispatcher();
            d.subscribe(sinkSubject, syncList::add);

            connectionProperties = defaultConnectionProperties(url);
            NatsSink<String> sink = newNatsSink(sinkSubject, connectionProperties, null);
            ds.sinkTo(sink);

//            ds.map(String::toUpperCase); //To Avoid Sink Dependency
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
            env.executeAsync("TestJsSourceBounded");

            Thread.sleep(12_000);
            env.close();
            ConsumerInfo ci = jsm.getConsumerInfo(streamName, consumerName);
            SequenceInfo sequenceInfo = ci.getDelivered();
            assertTrue(sequenceInfo.getStreamSequence() >= 2);

            for (Message m : syncList) {
                String payload = new String(m.getData());
            }
        });
    }

    @Test
    public void testJsSourceBounded() throws Exception {
        // Setup
        final String sourceSubject = random("sub");
        final String streamName = random("strm");
        final String consumerName = random("con");

        // Load connection properties
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("io.nats.client.url", "tls://platform-nats-internal-cluster.nxengg.cloud:443");
        connectionProperties.setProperty("keystore.path", "/Users/somratdutta/IdeaProjects/sdutta-nats-flink-connector/creds/keystore.jks");
        connectionProperties.setProperty("truststore.path", "/Users/somratdutta/IdeaProjects/sdutta-nats-flink-connector/creds/truststore.jks");
        connectionProperties.setProperty("store.password", "password");
        connectionProperties.setProperty("key.password", "password");

        // Create Secure NATS Connection
        try (Connection nc = createSecureConnection(connectionProperties)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // Create Stream and Publish Messages
            createStream(jsm, streamName, sourceSubject);
            publish(js, sourceSubject, 10);

            // Create Consumer
            createConsumer(jsm, streamName, sourceSubject, consumerName,-1);

            // Configure Flink NATS JetStream Source
            PayloadDeserializer<String> deserializer = new StringPayloadDeserializer();
            NatsJetStreamSource<String> natsSource = new NatsJetStreamSourceBuilder<String>()
                    .subjects(sourceSubject)
                    .payloadDeserializer(deserializer)
                    .connectionProperties(connectionProperties)
                    .consumerName(consumerName)
                    .build();

            // Configure Flink Environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getCheckpointConfig().setCheckpointInterval(10_000L);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));

            DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");

            // Terminal Output Sink
            ds.map(String::toUpperCase).map(msg -> {
                System.out.println("Received Message: " + msg);
                return msg;
            });

            env.execute("TestJsSourceBounded");

            // Verify Delivery
            ConsumerInfo ci = jsm.getConsumerInfo(streamName, consumerName);
            assertTrue(ci.getDelivered().getStreamSequence() >= 2);
        }
    }

    private Connection createSecureConnection(Properties properties) throws Exception {
        String keystorePath = properties.getProperty("keystore.path");
        String truststorePath = properties.getProperty("truststore.path");
        String storePassword = properties.getProperty("store.password");
        String keyPassword = properties.getProperty("key.password");

        SSLContext sslContext = createSSLContext(keystorePath, truststorePath, storePassword, keyPassword);

        // Add credentials file if needed
        String credsPath = "/Users/somratdutta/IdeaProjects/sdutta-nats-flink-connector/creds/stage.creds";

        Options options = new Options.Builder()
                .server(properties.getProperty("io.nats.client.url"))
                .sslContext(sslContext)
                .authHandler(Nats.credentials(credsPath)) // Add NATS credentials here
                .build();
        return Nats.connect(options);
    }

    private SSLContext createSSLContext(String keystorePath, String truststorePath, String storePassword, String keyPassword) throws Exception {
        // Load Keystore
        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(keystorePath))) {
            keyStore.load(in, storePassword.toCharArray());
        }

        // Load Truststore
        KeyStore trustStore = KeyStore.getInstance("JKS");
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(truststorePath))) {
            trustStore.load(in, storePassword.toCharArray());
        }

        // Initialize KeyManager
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(keyStore, keyPassword.toCharArray());

        // Initialize TrustManager
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(trustStore);

        // Create SSLContext
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
        return sslContext;
    }


    @Test
    public void testJsSourceUnbounded() throws Exception {
        String sourceSubject = random("sub");
        String streamName = random("strm");
        String consumerName = random("con");

        runInExternalServer(true, (nc, url) -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = jsm.jetStream();
            createStream(jsm, streamName, sourceSubject);

            ConsumerConfiguration cc = createConsumer(jsm, streamName, sourceSubject, consumerName, 5);
            // --------------------------------------------------------------------------------
            Properties connectionProperties = defaultConnectionProperties(url);
            PayloadDeserializer<String> deserializer = new StringPayloadDeserializer();
            NatsJetStreamSourceBuilder<String> builder = new NatsJetStreamSourceBuilder<String>()
                .subjects(sourceSubject)
                .payloadDeserializer(deserializer)
                .connectionProperties(connectionProperties)
                .consumerName(consumerName);

            // Flink environment setup
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<String> ds = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "nats-source-input");
            ds.map(String::toUpperCase);

            // Running Flink job in a separate thread
            Thread flinkThread = new Thread(() -> {
                try {
                    env.execute("TestJsSourceUnbounded");
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                catch (Exception e) {
                    fail(e);
                }
            });
            flinkThread.start();

            publish(js, sourceSubject, 5, 100);

            Thread.sleep(10000); // Increased sleep time to ensure messages are processed
            SequenceInfo sequenceInfo = nc.jetStream().getConsumerContext(streamName, consumerName).getConsumerInfo().getDelivered();
            assertTrue(sequenceInfo.getStreamSequence() >= 5);
            flinkThread.interrupt(); // Interrupt to stop the Flink job
        });
    }

    private static ConsumerConfiguration createConsumer(JetStreamManagement jsm, String streamName, String sourceSubject, String consumerName, int maxBatch) throws IOException, JetStreamApiException {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
            .durable(consumerName)
            .ackPolicy(AckPolicy.All)
            .filterSubject(sourceSubject)
            .maxBatch(5)
            .build();
        jsm.addOrUpdateConsumer(streamName, cc);
        return cc;
    }

    private static void createStream(JetStreamManagement jsm, String streamName, String sourceSubject) throws IOException, JetStreamApiException {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
            .name(streamName)
            .subjects(sourceSubject)
            .storageType(StorageType.Memory)
            .build();
        jsm.addStream(streamConfig);
    }
}

