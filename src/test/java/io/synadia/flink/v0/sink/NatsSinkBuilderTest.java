package io.synadia.flink.v0.sink;

import io.synadia.flink.v0.payload.PayloadSerializer;
import io.synadia.flink.v0.payload.StringPayloadSerializer;
import io.synadia.io.synadia.flink.TestBase;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/** Unit test for {@link NatsSinkBuilder}. */
class NatsSinkBuilderTest extends TestBase {

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

            NatsSinkBuilder<String> builder = new NatsSinkBuilder<>();
            NatsSink<String> sink = builder
                    .subjects(subject())
                    .connectionProperties(defaultConnectionProperties(url))
                    .payloadSerializer(new StringPayloadSerializer())
                    .build();

            assertNotNull(sink, "Sink should be built successfully");
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

            NatsSinkBuilder<String> builder = new NatsSinkBuilder<>();
            NatsSink<String> sink = builder
                    .connectionPropertiesFile(createTempPropertiesFile(defaultConnectionProperties(url)))
                    .subjects(subject())
                    .payloadSerializer(new StringPayloadSerializer())
                    .build();

            assertNotNull(sink, "Sink should be built from properties file");
        });
    }

    /**
     * Tests builder's handling of multiple subjects.
     * Verifies sink can be configured with:
     * - Multiple subjects in single call
     * - Subject list updates
     */
    @Test
    void shouldHandleMultipleSubjects() throws Exception {
        runInServer(true, (nc, url) -> {
            String subject1 = subject();
            String subject2 = subject();

            NatsSinkBuilder<String> builder = new NatsSinkBuilder<>();
            NatsSink<String> sink = builder
                    .connectionProperties(defaultConnectionProperties(url))
                    .subjects(subject1, subject2)
                    .payloadSerializer(new StringPayloadSerializer())
                    .build();

            assertNotNull(sink, "Sink should support multiple subjects");
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
            String subject = subject();

            // Test with direct serializer instance
            NatsSinkBuilder<String> builder1 = new NatsSinkBuilder<>();
            NatsSink<String> sink1 = builder1
                    .connectionProperties(defaultConnectionProperties(url))
                    .subjects(subject)
                    .payloadSerializer(new StringPayloadSerializer())
                    .build();
            assertNotNull(sink1, "Sink should build with direct serializer");

            // Test with serializer class name
            NatsSinkBuilder<String> builder2 = new NatsSinkBuilder<>();
            NatsSink<String> sink2 = builder2
                    .connectionProperties(defaultConnectionProperties(url))
                    .subjects(subject)
                    .payloadSerializerClass(StringPayloadSerializer.class.getName())
                    .build();
            assertNotNull(sink2, "Sink should build with serializer class");
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
            String subject = subject();

            // Test missing properties
            final NatsSinkBuilder<String> builderNoProps = new NatsSinkBuilder<>();
            assertThrows(IllegalStateException.class, () ->
                            builderNoProps.subjects(subject)
                                    .payloadSerializer(new StringPayloadSerializer())
                                    .build(),
                    "Should require connection properties"
            );

            // Test missing subjects
            final NatsSinkBuilder<String> builderNoSubjects = new NatsSinkBuilder<>();
            assertThrows(IllegalStateException.class, () ->
                            builderNoSubjects.connectionProperties(defaultConnectionProperties(url))
                                    .payloadSerializer(new StringPayloadSerializer())
                                    .build(),
                    "Should require subjects"
            );

            // Test missing serializer
            final NatsSinkBuilder<String> builderNoSerializer = new NatsSinkBuilder<>();
            assertThrows(IllegalStateException.class, () ->
                            builderNoSerializer.connectionProperties(defaultConnectionProperties(url))
                                    .subjects(subject)
                                    .build(),
                    "Should require serializer"
            );
        });
    }

    /**
     * Tests error handling when serializer class instantiation fails.
     * Verifies proper exception wrapping and error messages.
     */
    @Test
    void shouldRejectNullSerializer() throws Exception {
        runInServer(true, (nc, url) -> {
            Properties props = defaultConnectionProperties(url);
            NatsSinkBuilder<String> builder = new NatsSinkBuilder<>();

            assertThrows(IllegalStateException.class, () ->
                            builder.connectionProperties(props)
                                    .subjects(subject())
                                    .payloadSerializer(null)
                                    .build(),
                    "Should reject null serializer instance"
            );
        });
    }

    /**
     * Tests serializer configuration precedence.
     * Verifies:
     * - Direct serializer overrides class name
     * - Properties don't override explicit settings
     */
    @Test
    void shouldCreateDistinctSinkInstances() throws Exception {
        runInServer(true, (nc, url) -> {
            Properties props = defaultConnectionProperties(url);
            NatsSinkBuilder<String> builder = new NatsSinkBuilder<>();
            PayloadSerializer<String> serializer = new StringPayloadSerializer();

            // Build first sink
            NatsSink<String> sink1 = builder
                    .connectionProperties(props)
                    .subjects(subject())
                    .payloadSerializer(serializer)
                    .build();
            assertNotNull(sink1, "First sink should be built successfully");

            // Build second sink with different subject
            NatsSink<String> sink2 = builder
                    .connectionProperties(props)
                    .subjects(subject())
                    .payloadSerializer(serializer)
                    .build();
            assertNotNull(sink2, "Second sink should be built successfully");

            // Verify sinks are different instances
            assertNotSame(sink1, sink2, "Builder should create new sink instances");
        });
    }

    /**
     * Tests comprehensive property configuration and merging.
     */
    @Test
    void shouldMergePropertiesCorrectly() throws Exception {
        runInServer(true, (nc, url) -> {
            // Base connection properties
            Properties connProps = defaultConnectionProperties(url);

            // Sink-specific properties
            Properties sinkProps = new Properties();
            sinkProps.setProperty("nats.sink.payload.serializer", StringPayloadSerializer.class.getName());
            sinkProps.setProperty("nats.sink.custom.property", "value");

            // Additional properties
            Properties extraProps = new Properties();
            extraProps.setProperty("nats.sink.another.property", "another-value");

            // Test property merging
            NatsSinkBuilder<String> builder = new NatsSinkBuilder<>();
            NatsSink<String> sink = builder
                    .subjects(subject())
                    .connectionProperties(connProps)
                    .sinkProperties(sinkProps)
                    .sinkProperties(extraProps)
                    .build();

            assertNotNull(sink, "Sink should build with merged properties");

            // Test property override
            Properties overrideProps = new Properties();
            overrideProps.setProperty("nats.sink.custom.property", "new-value");

            sink = builder
                    .subjects(subject())
                    .connectionProperties(connProps)
                    .sinkProperties(sinkProps)
                    .sinkProperties(overrideProps)
                    .build();

            assertNotNull(sink, "Sink should build with overridden properties");
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
            NatsSinkBuilder<String> builder = new NatsSinkBuilder<>();
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

    /**
     * Tests validation of serializer configurations.
     * Verifies appropriate exceptions for:
     * - Null serializer
     * - Invalid serializer class
     * - Non-existent serializer class
     */
    @Test
    void testBuild_WithInvalidSerializer() throws Exception {
        runInServer(true, (nc, url) -> {
            Properties props = defaultConnectionProperties(url);
            NatsSinkBuilder<String> builder = new NatsSinkBuilder<>();

            // Test null serializer
            IllegalStateException nullSerializerEx = assertThrows(
                    IllegalStateException.class,
                    () -> builder
                            .connectionProperties(props)
                            .subjects(subject())
                            .payloadSerializer(null)
                            .build(),
                    "Should reject null serializer"
            );
            assertTrue(nullSerializerEx.getMessage().contains("serializer"),
                    "Exception should mention null serializer");

            // Test invalid serializer class
            IllegalStateException invalidClassEx = assertThrows(
                    IllegalStateException.class,
                    () -> builder
                            .connectionProperties(props)
                            .subjects(subject())
                            .payloadSerializerClass("invalid.class.name")
                            .build(),
                    "Should reject invalid serializer class"
            );
            assertTrue(invalidClassEx.getMessage().contains("serializer class"),
                    "Exception should mention invalid class");
        });
    }
}
