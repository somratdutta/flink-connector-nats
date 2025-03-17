package io.synadia.flink.v0.sink;

import io.synadia.flink.v0.payload.StringPayloadSerializer;
import io.synadia.flink.v0.payload.PayloadSerializer;
import io.synadia.io.synadia.flink.TestBase;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/** Unit test for {@link NatsJetStreamSinkBuilder}. */
class NatsJetStreamSinkBuilderTest extends TestBase {

    /**
     * Tests complete builder configuration with all required settings.
     * Verifies sink is properly constructed with:
     * - Connection properties
     * - Subject configuration
     * - Payload serializer
     */
    @Test
    void shouldBuildCompleteConfiguration() throws Exception {
        runInServer(true, (nc, url) -> {

            NatsJetStreamSinkBuilder<String> builder = new NatsJetStreamSinkBuilder<>();
            NatsJetStreamSink<String> sink = builder
                    .subjects(subject())
                    .connectionProperties(defaultConnectionProperties(url))
                    .payloadSerializer(new StringPayloadSerializer())
                    .build();

            assertNotNull(sink, "JetStream sink should be built successfully");
        });
    }

    /**
     * Tests builder configuration using properties file.
     * Verifies sink can be built using:
     * - Properties loaded from file
     * - Explicit subject setting
     * - Custom serializer
     */
    @Test
    void shouldBuildWithPropertiesFile() throws Exception {
        runInServer(true, (nc, url) -> {

            NatsJetStreamSinkBuilder<String> builder = new NatsJetStreamSinkBuilder<>();
            NatsJetStreamSink<String> sink = builder
                    .connectionPropertiesFile(createTempPropertiesFile(defaultConnectionProperties(url)))
                    .subjects(subject())
                    .payloadSerializer(new StringPayloadSerializer())
                    .build();

            assertNotNull(sink, "JetStream sink should be built from properties file");
        });
    }

    /**
     * Tests builder's handling of multiple subjects.
     * Verifies sink can be configured with multiple subjects.
     */
    @Test
    void shouldHandleMultipleSubjects() throws Exception {
        runInServer(true, (nc, url) -> {

            NatsJetStreamSinkBuilder<String> builder = new NatsJetStreamSinkBuilder<>();
            NatsJetStreamSink<String> sink = builder
                    .connectionProperties(defaultConnectionProperties(url))
                    .subjects(subject(), subject())
                    .payloadSerializer(new StringPayloadSerializer())
                    .build();

            assertNotNull(sink, "JetStream sink should support multiple subjects");
        });
    }

    /**
     * Tests builder's serializer configuration options.
     * Verifies both ways of setting serializer:
     * - Direct instance
     * - Via class name
     */
    @Test
    void shouldConfigureSerializer() throws Exception {
        runInServer(true, (nc, url) -> {

            // Test with direct serializer instance
            NatsJetStreamSinkBuilder<String> builder1 = new NatsJetStreamSinkBuilder<>();
            NatsJetStreamSink<String> sink1 = builder1
                    .connectionProperties(defaultConnectionProperties(url))
                    .subjects(subject())
                    .payloadSerializer(new StringPayloadSerializer())
                    .build();
            assertNotNull(sink1, "JetStream sink should build with direct serializer");

            // Test with serializer class name
            NatsJetStreamSinkBuilder<String> builder2 = new NatsJetStreamSinkBuilder<>();
            NatsJetStreamSink<String> sink2 = builder2
                    .connectionProperties(defaultConnectionProperties(url))
                    .subjects(subject())
                    .payloadSerializerClass(StringPayloadSerializer.class.getName())
                    .build();
            assertNotNull(sink2, "JetStream sink should build with serializer class");
        });
    }

    /**
     * Tests builder's validation of required properties.
     * Verifies appropriate exceptions for:
     * - Missing properties
     * - Missing subjects
     * - Missing serializer
     */
    @Test
    void shouldValidateRequiredProperties() throws Exception {
        runInServer(true, (nc, url) -> {

            // Test missing properties
            final NatsJetStreamSinkBuilder<String> builderNoProps = new NatsJetStreamSinkBuilder<>();
            assertThrows(IllegalStateException.class, () ->
                            builderNoProps.subjects(subject())
                                    .payloadSerializer(new StringPayloadSerializer())
                                    .build(),
                    "Should require connection properties"
            );

            // Test missing subjects
            final NatsJetStreamSinkBuilder<String> builderNoSubjects = new NatsJetStreamSinkBuilder<>();
            assertThrows(IllegalStateException.class, () ->
                            builderNoSubjects.connectionProperties(defaultConnectionProperties(url))
                                    .payloadSerializer(new StringPayloadSerializer())
                                    .build(),
                    "Should require subjects"
            );

            // Test missing serializer
            final NatsJetStreamSinkBuilder<String> builderNoSerializer = new NatsJetStreamSinkBuilder<>();
            assertThrows(IllegalStateException.class, () ->
                            builderNoSerializer.connectionProperties(defaultConnectionProperties(url))
                                    .subjects(subject())
                                    .build(),
                    "Should require serializer"
            );
        });
    }

    /**
     * Tests sinkProperties method for setting properties.
     * Verifies:
     * - Basic properties are set
     * - Serializer class is configured from properties
     */
    @Test
    void shouldConfigureFromSinkProperties() throws Exception {
        runInServer(true, (nc, url) -> {
            Properties props = defaultConnectionProperties(url);
            props.setProperty("nats.sink.payload.serializer", StringPayloadSerializer.class.getName());

            NatsJetStreamSinkBuilder<String> builder = new NatsJetStreamSinkBuilder<>();
            NatsJetStreamSink<String> sink = builder
                    .subjects(subject())
                    .connectionProperties(props)
                    .sinkProperties(props)
                    .build();

            assertNotNull(sink, "JetStream sink should be built with properties");
        });
    }

    /**
     * Tests error handling when serializer class instantiation fails.
     * Verifies proper exception wrapping and error messages.
     */
    @Test
    void shouldHandleSerializerInstantiationFailure() throws Exception {
        runInServer(true, (nc, url) -> {
            Properties props = defaultConnectionProperties(url);

            NatsJetStreamSinkBuilder<String> builder = new NatsJetStreamSinkBuilder<>();

            // Test with non-existent class
            IllegalStateException thrown = assertThrows(IllegalStateException.class, () ->
                            builder.connectionProperties(props)
                                    .subjects(subject())
                                    .payloadSerializerClass("non.existent.Class")
                                    .build(),
                    "Should throw when serializer class doesn't exist"
            );
            assertTrue(thrown.getMessage().contains("Valid payload serializer class must be provided"));
            assertNotNull(thrown.getCause(), "Should include cause exception");

            // Test with class that's not a PayloadSerializer
            thrown = assertThrows(IllegalStateException.class, () ->
                            builder.connectionProperties(props)
                                    .subjects(subject())
                                    .payloadSerializerClass("java.lang.String")
                                    .build(),
                    "Should throw when class is not a PayloadSerializer"
            );
            assertTrue(thrown.getMessage().contains("Valid payload serializer class must be provided"));
            assertNotNull(thrown.getCause(), "Should include cause exception");
        });
    }

    /**
     * Tests serializer configuration precedence.
     * Verifies:
     * - Direct serializer overrides class name
     * - Properties don't override explicit settings
     */
    @Test
    void shouldRespectSerializerPrecedence() throws Exception {
        runInServer(true, (nc, url) -> {
            Properties props = defaultConnectionProperties(url);
            props.setProperty("nats.sink.payload.serializer", "some.other.Serializer");

            // Direct serializer should take precedence over properties
            NatsJetStreamSinkBuilder<String> builder = new NatsJetStreamSinkBuilder<>();
            NatsJetStreamSink<String> sink = builder
                    .connectionProperties(props)
                    .subjects(subject())
                    .payloadSerializer(new StringPayloadSerializer())
                    .build();
            assertNotNull(sink, "JetStream sink should build with direct serializer");

            // Explicit class name should take precedence over properties
            builder = new NatsJetStreamSinkBuilder<>();
            sink = builder
                    .connectionProperties(props)
                    .subjects(subject())
                    .payloadSerializerClass(StringPayloadSerializer.class.getName())
                    .build();
            assertNotNull(sink, "JetStream sink should build with explicit serializer class");
        });
    }

    /**
     * Tests validation of subject configurations.
     * Verifies appropriate exceptions for:
     * - Empty subject string
     * - Null subject array
     * - Invalid subject patterns
     */
    @Test
    void testBuild_WithInvalidSubjects() throws Exception {
        runInServer(true, (nc, url) -> {
            Properties props = defaultConnectionProperties(url);
            NatsJetStreamSinkBuilder<String> builder = new NatsJetStreamSinkBuilder<>();
            PayloadSerializer<String> serializer = new StringPayloadSerializer();

            // Test empty subject string
            IllegalStateException emptySubjectEx = assertThrows(
                    IllegalStateException.class,
                    () -> builder
                            .connectionProperties(props)
                            .subjects("")
                            .payloadSerializer(serializer)
                            .build(),
                    "Should reject empty subject"
            );
            assertTrue(emptySubjectEx.getMessage().contains("subject"),
                    "Exception should mention invalid subject");

            // Test null subjects array
            IllegalStateException nullSubjectsEx = assertThrows(
                    IllegalStateException.class,
                    () -> builder
                            .connectionProperties(props)
                            .subjects((String[])null)
                            .payloadSerializer(serializer)
                            .build(),
                    "Should reject null subjects array"
            );
            assertTrue(nullSubjectsEx.getMessage().contains("Subjects"),
                    "Exception should mention missing subjects");

            // Test invalid subject pattern
            IllegalStateException invalidPatternEx = assertThrows(
                    IllegalStateException.class,
                    () -> builder
                            .connectionProperties(props)
                            .subjects("invalid..subject")
                            .payloadSerializer(serializer)
                            .build(),
                    "Should reject invalid subject pattern"
            );
            assertTrue(invalidPatternEx.getMessage().contains("pattern"),
                    "Exception should mention invalid pattern");
        });
    }
}
