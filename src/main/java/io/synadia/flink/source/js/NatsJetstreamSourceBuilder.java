// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.js;

import static io.synadia.flink.Constants.SOURCE_STARTUP_JITTER_MAX;
import static io.synadia.flink.Constants.SOURCE_STARTUP_JITTER_MIN;
import static io.synadia.flink.Constants.SOURCE_SUBJECTS;
import io.synadia.flink.Utils;
import io.synadia.flink.common.NatsSinkOrSourceBuilder;
import io.synadia.flink.payload.PayloadDeserializer;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;

public class NatsJetstreamSourceBuilder<OutputT> {

    private PayloadDeserializer<OutputT> deserializationSchema;

    /**
     * Set the deserializer for the source.
     * @param deserializationSchema the deserializer.
     * @return the builder
     */
    public NatsJetstreamSourceBuilder<OutputT> payloadDeserializer(PayloadDeserializer<OutputT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    /**
     * Build a NatsSource. Subject and
     * @return the source
     */
    public NatsJetstreamSource<OutputT> build() {
        if (deserializationSchema == null) {
                throw new IllegalStateException("Valid payload serializer class must be provided.");
        }
        return new NatsJetstreamSource<>(deserializationSchema);
    }
}
