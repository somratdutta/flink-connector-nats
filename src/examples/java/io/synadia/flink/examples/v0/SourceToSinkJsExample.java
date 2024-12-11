package io.synadia.flink.examples.v0;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.utils.PropertiesUtils;
import io.synadia.flink.v0.NatsJetStreamSource;
import io.synadia.flink.v0.NatsJetStreamSourceBuilder;
import io.synadia.flink.v0.payload.StringPayloadDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Properties;

public class SourceToSinkJsExample {
    public static void main(String[] args) throws Exception {
        // Load configuration from application.properties
        //Properties props = PropertiesUtils.loadPropertiesFromFile("/Users/somratdutta/IdeaProjects/sdutta-nats-flink-connector/src/test/java/io/synadia/io/synadia/flink/v0/nats-cloud-application.properties");
        Properties props = PropertiesUtils.loadPropertiesFromFile("src/examples/resources/application.properties");

        // Define static names loaded from properties
        String sourceSubject = props.getProperty("source.JsSubject");
        String streamName = props.getProperty("source.stream");
        String consumerName = props.getProperty("source.consumer");

        // Connect to NATS server
        Connection nc = connect(props);
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

        // Create a JetStream stream for the source subject
        createStream(jsm, streamName, sourceSubject);

        // Publish test messages to the source subject
        publish(js, sourceSubject, 10);

        // Create a consumer for the JetStream source
        createConsumer(jsm, streamName, sourceSubject, consumerName);

        // Configure the NATS JetStream Source
        Properties connectionProperties = props;
        StringPayloadDeserializer deserializer = new StringPayloadDeserializer();
        NatsJetStreamSourceBuilder<String> builder = new NatsJetStreamSourceBuilder<String>()
                .subjects(sourceSubject)
                .payloadDeserializer(deserializer) // Deserialize messages from source
                .connectionProperties(connectionProperties)
                .consumerName(consumerName);

        NatsJetStreamSource<String> natsSource = builder.build();

        // Configure Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(10_000L); // Set checkpoint interval

        // Create the data stream from NATS JetStream Source
        DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");

        // Simple transformation: Convert to uppercase and print to the terminal
        ds.map(String::toUpperCase) // Transformation to uppercase
                .map(message -> {
                    System.out.println("Transformed message: " + message);
                    return message;
                });

        // Configure Flink restart strategy
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));

        // Execute Flink pipeline asynchronously
        env.execute("JetStream Source-to-Terminal Example");

        // Clean up NATS resources
        jsm.deleteStream(streamName);
        System.out.println("Stream deleted: " + streamName);

        // Close the NATS connection
        nc.close();
        System.exit(0);
    }

    /**
     * Connect to the NATS server using provided properties.
     */
    private static Connection connect(Properties props) throws Exception {
        // Build NATS Options
        Options.Builder builder = new Options.Builder();

        // Set the server URL
        String serverUrl = props.getProperty("io.nats.client.url");
        if (serverUrl == null || serverUrl.isEmpty()) {
            throw new IllegalArgumentException("NATS server URL is not specified in properties.");
        }
        builder.server(serverUrl);

        /*// Set TLS truststore if provided
        String trustStorePath = props.getProperty("io.nats.client.trustStorePath");
        String trustStorePassword = props.getProperty("io.nats.client.trustStorePassword");
        if (trustStorePath != null && !trustStorePath.isEmpty()) {
            builder.truststorePath(trustStorePath);
            if (trustStorePassword != null && !trustStorePassword.isEmpty()) {
                builder.truststorePassword(trustStorePassword.toCharArray());
            }
        }

        // Set credentials for JWT authentication
        String credsPath = props.getProperty("io.nats.client.credsPath");
        if (credsPath != null && !credsPath.isEmpty()) {
            builder.authHandler(Nats.credentials(credsPath));
        }*/

        // Build and connect to NATS
        try {
            return Nats.connect(builder.build());
        } catch (Exception e) {
            throw new IOException("Unable to connect to NATS server.", e);
        }
    }

    /**
     * Create a JetStream stream with the specified name and subject.
     */
    private static void createStream(JetStreamManagement jsm, String streamName, String subject) throws Exception {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(streamName)
                .subjects(subject)
                .build();
        jsm.addStream(streamConfig);
        System.out.println("Stream created: " + streamName);
    }

    /**
     * Create a durable consumer for the given stream and subject.
     */
    private static void createConsumer(JetStreamManagement jsm, String streamName, String subject, String consumerName) throws Exception {
        ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                .durable(consumerName) // Durable consumer for persistence
                .ackPolicy(AckPolicy.All) // Explicit acknowledgement policy
                .filterSubject(subject) // Filter messages for this subject
                .build();
        jsm.addOrUpdateConsumer(streamName, consumerConfig);
        System.out.println("Consumer created: " + consumerName);
    }

    /**
     * Publish a fixed number of test messages to the specified JetStream subject.
     */
    private static void publish(JetStream js, String subject, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            js.publish(subject, ("Message " + i).getBytes());
        }
    }
}
