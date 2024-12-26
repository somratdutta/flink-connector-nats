package io.synadia.io.synadia.flink.v0.E2ETests;

import io.nats.client.*;
import io.nats.client.api.*;
import io.synadia.flink.v0.payload.StringPayloadDeserializer;
import io.synadia.flink.v0.payload.StringPayloadSerializer;
import io.synadia.flink.v0.sink.NatsSink;
import io.synadia.flink.v0.sink.NatsSinkBuilder;
import io.synadia.flink.v0.source.NatsJetStreamSourceBuilder;
import io.synadia.io.synadia.flink.TestBase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.time.Duration;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Function;

import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NatsJetStreamE2ECase extends TestBase {


    @Test
    public void testBoundedSourceProcessing() throws Exception {
        runInExternalServer(true, (nc, url) -> {
            String streamName = stream();
            String sourceSubject = subject();
            String sinkSubject = subject();
            String consumerName = consumer();
            final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());

            // Setup stream and consumer
            JetStreamManagement jsm = nc.jetStreamManagement();
            createStream(jsm, streamName, sourceSubject);
            createConsumer(jsm, streamName, sourceSubject, consumerName);

            // Publish messages
            JetStream js = nc.jetStream();
            int messageCount = 100;
            publish(js, sourceSubject, messageCount);

            // Create and execute Flink job
            StreamExecutionEnvironment env = getStreamExecutionEnvironment();
            env.getCheckpointConfig().setCheckpointInterval(10_000L);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));

            // Configure source
            Properties connectionProperties = defaultConnectionProperties(url);
            NatsJetStreamSourceBuilder<String> sourceBuilder = new NatsJetStreamSourceBuilder<String>()
                    .subjects(sourceSubject)
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .connectionProperties(connectionProperties)
                    .consumerName(consumerName)
                    .maxFetchRecords(100)
                    .maxFetchTime(Duration.ofSeconds(5))
                    .boundness(Boundedness.BOUNDED);

            // Setup sink to collect results
            Dispatcher dispatcher = nc.createDispatcher();
            dispatcher.subscribe(sinkSubject, syncList::add);

            // Configure and execute pipeline
            NatsSink<String> sink = new NatsSinkBuilder<String>()
                    .subjects(sinkSubject)
                    .connectionProperties(connectionProperties)
                    .payloadSerializer(new StringPayloadSerializer())
                    .build();

            env.fromSource(sourceBuilder.build(), WatermarkStrategy.noWatermarks(), "NATS Source")
                    .map(String::toUpperCase)
                    .sinkTo(sink);

            JobClient jobClient = env.executeAsync("TestBoundedSource");

            // Wait for processing to complete
            sleep(12_000);

            // Verify results
            assertEquals(messageCount, syncList.size(),
                    "All " + messageCount + " messages should be received at the sink.");

            // Cleanup
            jobClient.cancel().get();
            env.close();
            jsm.deleteStream(streamName);
        });
    }

    @Test
    public void testUnboundedSourceProcessing() throws Exception {
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());
        String sourceSubject = subject();
        String sinkSubject = subject();
        String streamName = stream();
        String consumerName = consumer();

        runInExternalServer(true, (nc, url) -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = jsm.jetStream();

            // Step 1: Create the source stream and consumer
            createStream(jsm, streamName, sourceSubject);
            createConsumer(jsm, streamName, sourceSubject, consumerName);

            // Step 2: Configure NATS JetStream Source
            Properties connectionProperties = defaultConnectionProperties(url);
            NatsJetStreamSourceBuilder<String> builder = new NatsJetStreamSourceBuilder<String>()
                    .subjects(sourceSubject)
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .connectionProperties(connectionProperties)
                    .consumerName(consumerName)
                    .maxFetchRecords(100)
                    .maxFetchTime(Duration.ofSeconds(5))
                    .boundness(Boundedness.CONTINUOUS_UNBOUNDED);

            // Step 3: Set up Flink environment
            StreamExecutionEnvironment env = getStreamExecutionEnvironment();
            env.getCheckpointConfig().setCheckpointInterval(10_000L);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));

            // Step 4: Setup sink to collect results
            Dispatcher dispatcher = nc.createDispatcher();
            dispatcher.subscribe(sinkSubject, syncList::add);

            // Step 5: Configure and execute pipeline
            NatsSink<String> sink = new NatsSinkBuilder<String>()
                    .subjects(sinkSubject)
                    .connectionProperties(connectionProperties)
                    .payloadSerializer(new StringPayloadSerializer())
                    .build();

            env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "nats-source-input")
                    .map(String::toUpperCase)
                    .sinkTo(sink);

            // Step 6: Execute the job asynchronously
            JobClient jobClient = env.executeAsync("TestJsSourceUnbounded");

            // Step 7: Publish messages with delay
            publish(js, sourceSubject, 5, 100);

            // Step 8: Wait for processing
            sleep(10_000);

            // Step 9: Verify results
            assertEquals(5, syncList.size(), "All 5 messages should be received at the sink.");

            // Step 10: Cleanup
            try {
                jobClient.cancel().get();
                System.out.println("Flink job canceled successfully.");
            } catch (Exception e) {
                e.printStackTrace();
                fail("Failed to cancel Flink job: " + e.getMessage());
            }

            env.close();
            jsm.deleteStream(streamName);
        });
    }
    //TODO Test is broken, validate and fix
    @Test
    public void testCheckpointRestore() throws Exception {
        final List<String> receivedMessages = Collections.synchronizedList(new ArrayList<>());
        String sourceSubject = random("sub");
        String sinkSubject = random("sink");
        String streamName = random("strm");
        String consumerName = "test-consumer";

        runInExternalServer(true, (nc, url) -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = jsm.jetStream();

            // Step 1: Create stream with explicit configuration
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(streamName)
                    .subjects(sourceSubject)
                    .storageType(StorageType.Memory)
                    .replicas(1)
                    .build();
            jsm.addStream(streamConfig);

            // Step 2: Configure consumer without queue/deliver group settings
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(consumerName)
                    .filterSubject(sourceSubject)
                    .ackPolicy(AckPolicy.Explicit)
                    .deliverPolicy(DeliverPolicy.All)
                    .replayPolicy(ReplayPolicy.Instant)
                    .build();
            jsm.addOrUpdateConsumer(streamName, cc);

            // Step 3: Configure source with pull-based settings
            Properties connectionProperties = defaultConnectionProperties(url);
            NatsJetStreamSourceBuilder<String> builder = new NatsJetStreamSourceBuilder<String>()
                    .subjects(sourceSubject)
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .connectionProperties(connectionProperties)
                    .consumerName(consumerName)
                    .maxFetchRecords(100)
                    .maxFetchTime(Duration.ofSeconds(5))
                    .boundness(Boundedness.CONTINUOUS_UNBOUNDED);

            // Step 4: Create pipeline function
            Function<StreamExecutionEnvironment, JobClient> createPipeline = (env) -> {
                env.enableCheckpointing(1000);
                env.setParallelism(1);

                // Build pipeline with message tracking
                env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "nats-source")
                        .map(message -> {
                            receivedMessages.add(message);
                            return message.toUpperCase();
                        })
                        .sinkTo(new NatsSinkBuilder<String>()
                                .subjects(sinkSubject)
                                .connectionProperties(connectionProperties)
                                .payloadSerializer(new StringPayloadSerializer())
                                .build());

                // Execute the job
                try {
                    return env.executeAsync("TestCheckpointRestore");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };

            // Step 5: Execute first job
            JobClient jobClient = createPipeline.apply(getStreamExecutionEnvironment());

            // Publish messages and verify
            for (int i = 0; i < 5; i++) {
                js.publish(sourceSubject, ("message-" + i).getBytes());
                sleep(500);
            }

            sleep(5000);

            // Verify first batch processed
            assertTrue(receivedMessages.size() > 0,
                    "Should have processed messages in first job, got " + receivedMessages.size());
            int firstJobMessages = receivedMessages.size();

            // Cancel first job but keep consumer
            jobClient.cancel().get();
            sleep(2000);

            // Step 6: Start second job
            jobClient = createPipeline.apply(getStreamExecutionEnvironment());

            // Publish more messages
            for (int i = 5; i < 10; i++) {
                js.publish(sourceSubject, ("message-" + i).getBytes());
                sleep(500);
            }

            sleep(5000);

            // Verify second batch processed
            assertTrue(receivedMessages.size() > firstJobMessages,
                    "Second job should have processed additional messages. First job: " +
                            firstJobMessages + ", Total: " + receivedMessages.size());

            // Cleanup
            jobClient.cancel().get();
            jsm.deleteConsumer(streamName, consumerName);
            jsm.deleteStream(streamName);
        });
    }

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

    private void publish(JetStream js, String subject, int count, int startFrom, long delayMs) throws Exception {
        for (int i = startFrom; i < startFrom + count; i++) {
            js.publish(subject, ("data-" + i).getBytes());
            if (delayMs > 0) {
                sleep(delayMs);
            }
        }
    }

    private static ConsumerConfiguration createConsumer(JetStreamManagement jsm, String streamName, String sourceSubject, String consumerName) throws IOException, JetStreamApiException {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(consumerName)
                .ackPolicy(AckPolicy.All)
                .filterSubject(sourceSubject)
                .build();
        jsm.addOrUpdateConsumer(streamName, cc);
        return cc;
    }

    private static void createStream(JetStreamManagement jsm, String streamName, String sourceSubject) throws IOException, JetStreamApiException {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(streamName)
                .subjects(sourceSubject)
                .build();
        jsm.addStream(streamConfig);
    }


} 