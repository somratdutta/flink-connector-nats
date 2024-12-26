package io.synadia.io.synadia.flink.v0.unitTests;

import io.nats.client.JetStreamManagement;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.v0.source.NatsJetStreamSourceBuilder;
import io.synadia.flink.v0.payload.StringPayloadDeserializer;
import io.synadia.io.synadia.flink.TestBase;
import org.apache.flink.api.connector.source.Boundedness;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
//TODO verify if this is the correct
public class NatsJetStreamSourceConfigurationTest extends TestBase {

    @Test
    void testValidConfiguration() throws Exception {
        runInExternalServer(true, (nc, url) -> {
            String consumerName = random("consumer");
            String subject = random("subject");
            String streamName = random("stream");

            // Create stream
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(streamName)
                    .subjects(subject)
                    .storageType(StorageType.Memory)
                    .replicas(1)
                    .build();
            jsm.addStream(streamConfig);

            // Should not throw any exceptions
            assertDoesNotThrow(() -> new NatsJetStreamSourceBuilder<String>()
                    .consumerName(consumerName)
                    .subjects(subject)
                    .connectionProperties(defaultConnectionProperties(url))
                    .messageQueueCapacity(1000)
                    .enableAutoAcknowledgeMessage(false)
                    .fetchOneMessageTime(Duration.ofMillis(100))
                    .maxFetchRecords(100)
                    .natsAutoAckInterval(Duration.ofSeconds(5))
                    .boundness(Boundedness.CONTINUOUS_UNBOUNDED)
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .build());
        });
    }

    @Test
    void testInvalidConfiguration() throws Exception {
        runInExternalServer(true, (nc, url) -> {
            String subject = random("subject");
            String streamName = random("stream");

            // Create stream
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(streamName)
                    .subjects(subject)
                    .storageType(StorageType.Memory)
                    .replicas(1)
                    .build();
            jsm.addStream(streamConfig);

            // Test missing required parameters
            assertThrows(IllegalStateException.class, () ->
                    new NatsJetStreamSourceBuilder<String>().build());

            // Test missing consumer name
            assertThrows(IllegalStateException.class, () ->
                    new NatsJetStreamSourceBuilder<String>()
                            .subjects(subject)
                            .connectionProperties(defaultConnectionProperties(url))
                            .payloadDeserializer(new StringPayloadDeserializer())
                            .build());

            // Test missing subjects
            assertThrows(IllegalStateException.class, () ->
                    new NatsJetStreamSourceBuilder<String>()
                            .consumerName(random("consumer"))
                            .connectionProperties(defaultConnectionProperties(url))
                            .payloadDeserializer(new StringPayloadDeserializer())
                            .build());

            // Test missing connection properties
            assertThrows(IllegalStateException.class, () ->
                    new NatsJetStreamSourceBuilder<String>()
                            .consumerName(random("consumer"))
                            .subjects(subject)
                            .payloadDeserializer(new StringPayloadDeserializer())
                            .build());

            // Test missing deserializer
            assertThrows(IllegalStateException.class, () ->
                    new NatsJetStreamSourceBuilder<String>()
                            .consumerName(random("consumer"))
                            .subjects(subject)
                            .connectionProperties(defaultConnectionProperties(url))
                            .build());
        });
    }
}