package io.synadia.flink.source.js;

import static io.synadia.flink.Constants.SOURCE_STARTUP_JITTER_MAX;
import static io.synadia.flink.Constants.SOURCE_STARTUP_JITTER_MIN;
import static io.synadia.flink.Constants.SOURCE_SUBJECTS;
import io.synadia.flink.Utils;
import io.synadia.flink.common.NatsSinkOrSourceBuilder;
import io.synadia.flink.payload.PayloadDeserializer;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.connector.source.Boundedness;

public class NatsJetStreamSourceBuilder<OutputT> extends NatsSinkOrSourceBuilder<NatsJetStreamSourceBuilder<OutputT>> {

    private PayloadDeserializer<OutputT> payloadDeserializer;
    private NatsConsumerConfig natsConsumeOptions;
    private Boundedness mode = Boundedness.BOUNDED; // default

    @Override
    protected NatsJetStreamSourceBuilder<OutputT> getThis() {
        return this;
    }

    /**
     * Set the deserializer for the source.
     * @param payloadDeserializer the deserializer.
     * @return the builder
     */
    public NatsJetStreamSourceBuilder<OutputT> payloadDeserializer(PayloadDeserializer<OutputT> payloadDeserializer) {
        this.payloadDeserializer = payloadDeserializer;
        return this;
    }

    /**
     * Set source properties from a properties object
     * See the readme and {@link io.synadia.flink.Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public NatsJetStreamSourceBuilder<OutputT> sourceProperties(Properties properties) {
        List<String> subjects = Utils.getPropertyAsList(properties, SOURCE_SUBJECTS);
        if (!subjects.isEmpty()) {
            subjects(subjects);
        }

        long l = Utils.getLongProperty(properties, SOURCE_STARTUP_JITTER_MIN, -1);
        if (l != -1) {
            minConnectionJitter(l);
        }

        l = Utils.getLongProperty(properties, SOURCE_STARTUP_JITTER_MAX, -1);
        if (l != -1) {
            maxConnectionJitter(l);
        }

        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> consumerConfig(NatsConsumerConfig config) {
        this.natsConsumeOptions = config;
        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> boundedness(Boundedness mode) {
        this.mode = mode;
        return this;
    }

    /**
     * Build a NatsSource.
     * @return the source
     */
    public NatsJetStreamSource<OutputT> build() {
        beforeBuild();
        if (payloadDeserializer == null) {
            throw new IllegalStateException("Valid payload deserializer class must be provided.");
        }
        return new NatsJetStreamSource<>(payloadDeserializer, createConnectionFactory(), subjects.get(0), natsConsumeOptions, mode);
    }
}
