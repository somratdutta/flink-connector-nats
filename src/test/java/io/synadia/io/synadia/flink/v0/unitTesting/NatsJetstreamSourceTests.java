package io.synadia.io.synadia.flink.v0.unitTesting;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.nats.client.Message;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.NatsJetStreamSource;
import io.synadia.flink.v0.NatsJetStreamSourceBuilder;
import io.synadia.flink.v0.source.split.NatsSubjectSplit;
import io.synadia.flink.v0.source.reader.NatsSourceFetcherManager;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

class NatsJetStreamSourceTests {

    private NatsJetStreamSource<String> source;
    private PayloadDeserializer<String> mockPayloadDeserializer;
    private SourceReaderContext mockReaderContext;
    private FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> mockQueue;
    private Supplier<SplitReader<Message, NatsSubjectSplit>> mockSplitReaderSupplier;

    @BeforeEach
    void setUp() {
        // Mock dependencies
        mockPayloadDeserializer = mock(PayloadDeserializer.class);
        mockReaderContext = mock(SourceReaderContext.class);

        // Initialize FutureCompletingBlockingQueue with a valid backing queue
        mockQueue = new FutureCompletingBlockingQueue<>();

        // Mock SplitReader
        SplitReader<Message, NatsSubjectSplit> mockSplitReader = mock(SplitReader.class);
        mockSplitReaderSupplier = () -> mockSplitReader;

        // Mock reader context configuration
        Configuration mockConfiguration = new Configuration();
        when(mockReaderContext.getConfiguration()).thenReturn(mockConfiguration);


        // Create connection properties
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("io.nats.client.url", "nats://localhost:4222");

        // Use the builder to create the source
        source = new NatsJetStreamSourceBuilder<String>()
                .subjects("testSubject") // Set subject
                .payloadDeserializer(mockPayloadDeserializer)
                .connectionProperties(connectionProperties) // Set connection properties
                .consumerName("testConsumer") // Required consumer name
                .build();
    }

    @Test
    void testInitialization() {
        assertNotNull(source, "Source should be initialized");
    }

    @Test
    void testGetBoundedness() {
        Boundedness boundedness = source.getBoundedness();
        assertNull(boundedness, "Boundedness should return null (default behavior)");
    }

/*    @Test
    void testCreateReader() throws Exception {
        // Create the fetcher manager with proper mocks
        NatsSourceFetcherManager fetcherManager = new NatsSourceFetcherManager(
                mockQueue,
                mockSplitReaderSupplier,
                new Configuration()
        );

        // Inject fetcher manager into the source and create the reader
        SourceReader<String, NatsSubjectSplit> reader = source.createReader(mockReaderContext);

        // Assert
        assertNotNull(reader, "SourceReader should be created successfully");
        assertTrue(reader instanceof SourceReader, "Reader should be of the correct type");

        // Verify interactions with mocked context
        verify(mockReaderContext).getConfiguration();
    }*/

    @Test
    void testCreateReaderHandlesNullContext() {
        assertThrows(
                NullPointerException.class,
                () -> source.createReader(null),
                "Creating a reader with null context should throw an exception"
        );
    }

    @Test
    void testBuilderThrowsWithoutConsumerName() {
        // Act and Assert
        Exception exception = assertThrows(
                IllegalStateException.class,
                () -> new NatsJetStreamSourceBuilder<String>()
                        .subjects("testSubject") // Set subject
                        .payloadDeserializer(mockPayloadDeserializer)
                        .connectionProperties(new Properties()) // Set empty properties
                        .build()
        );
        assertTrue(exception.getMessage().contains("Consumer name must be provided."),
                "Exception message should indicate missing consumer name");
    }
}
