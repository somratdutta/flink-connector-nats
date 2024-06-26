// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.io.synadia.flink;

import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.*;
import io.nats.client.support.SerializableConsumerConfiguration;
import io.synadia.flink.source.NatsJetStreamSource;
import io.synadia.flink.source.NatsJetStreamSourceBuilder;
import io.synadia.flink.payload.StringPayloadDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class NatsJetStreamSourceTest extends TestBase {

    @Test
    public void testSourceBounded() throws Exception {
        String sourceSubject1 = "test";
        String streamName = "test";
        String consumerName = "testconsumer";

        runInExternalServer(true, (nc, url) -> {

            // publish to the source's subjects
            StreamConfiguration stream = new StreamConfiguration.Builder()
                    .name(streamName)
                    .subjects(sourceSubject1)
                    .build();
            nc.jetStreamManagement().addStream(stream);

            // Create and configure a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(consumerName)
                    .ackPolicy(AckPolicy.All)
                    .filterSubject(sourceSubject1)
                    .maxBatch(5)
                    .build();
            nc.jetStreamManagement().addOrUpdateConsumer(streamName, cc);

            // Publish messages
            nc.jetStream().publish(sourceSubject1, "Hi".getBytes());
            nc.jetStream().publish(sourceSubject1, "Hello".getBytes());

            // --------------------------------------------------------------------------------
            Properties connectionProperties = defaultConnectionProperties(url);
            StringPayloadDeserializer deserializer = new StringPayloadDeserializer();
            SerializableConsumerConfiguration consumerConfig = new SerializableConsumerConfiguration(cc);

            NatsJetStreamSourceBuilder<String> builder = new NatsJetStreamSourceBuilder<String>()
                    .subjects(sourceSubject1)
                    .payloadDeserializer(deserializer)
                    .boundedness(Boundedness.BOUNDED)
                    .consumerConfig(consumerConfig);
            builder.connectionProperties(connectionProperties);

            NatsJetStreamSource<String> natsSource = builder.build();
            StreamExecutionEnvironment env = getStreamExecutionEnvironment();
            DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-flink-bounded");
            ds.map(String::toUpperCase); // To Avoid Sink Dependency
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
            env.executeAsync();
            Thread.sleep(15000);
            env.close();
            SequenceInfo sequenceInfo = nc.jetStream().getConsumerContext(sourceSubject1, consumerName).getConsumerInfo().getDelivered();
            assertTrue(sequenceInfo.getStreamSequence() >= 2);
        });
    }

    @Test
    public void testSourceUnbounded() throws Exception {
        String sourceSubject = "test";
        String streamName = "test";
        String consumerName = "testconsumer";

        runInExternalServer(true, (nc, url) -> {
            // NATS setup
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(streamName)
                    .subjects(sourceSubject)
                    .build();
            jsm.addStream(streamConfig);
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(consumerName)
                    .ackPolicy(AckPolicy.All)
                    .filterSubject(sourceSubject)
                    .maxBatch(5)
                    .build();
            jsm.addOrUpdateConsumer(streamName, cc);

            // Flink environment setup
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StringPayloadDeserializer deserializer = new StringPayloadDeserializer();
            Properties connectionProperties = defaultConnectionProperties(url);
            SerializableConsumerConfiguration consumerConfig = new SerializableConsumerConfiguration(cc);

            NatsJetStreamSourceBuilder<String> builder = new NatsJetStreamSourceBuilder<String>()
                    .subjects(sourceSubject).payloadDeserializer(deserializer)
                    .boundedness(Boundedness.CONTINUOUS_UNBOUNDED).consumerConfig(consumerConfig);
            builder.connectionProperties(connectionProperties);
            DataStream<String> ds = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "nats-source-input");
            ds.map(String::toUpperCase);

            // Running Flink job in a separate thread
            Thread flinkThread = new Thread(() -> {
                try {
                    env.execute("nats-flink-unbounded");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            flinkThread.start();

            // Publish messages
            JetStream js = nc.jetStream();
            for (int i = 0; i < 5; i++) {
                js.publish(sourceSubject, ("Message " + i).getBytes());
                Thread.sleep(100); // Wait between messages
            }
            Thread.sleep(10000); // Increased sleep time to ensure messages are processed
            SequenceInfo sequenceInfo = nc.jetStream().getConsumerContext(sourceSubject, consumerName).getConsumerInfo().getDelivered();
            assertTrue(sequenceInfo.getStreamSequence() >= 5);
            flinkThread.interrupt(); // Interrupt to stop the Flink job
        });
    }
}
