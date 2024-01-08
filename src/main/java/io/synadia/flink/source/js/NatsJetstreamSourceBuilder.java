// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.js;

import static io.synadia.flink.Constants.SOURCE_STARTUP_JITTER_MAX;
import static io.synadia.flink.Constants.SOURCE_STARTUP_JITTER_MIN;
import static io.synadia.flink.Constants.SOURCE_SUBJECTS;
import io.nats.client.api.ConsumerConfiguration;
import io.synadia.flink.Utils;
import io.synadia.flink.common.NatsSinkOrSourceBuilder;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.SourceConfiguration;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;

public class NatsJetstreamSourceBuilder<OutputT> {

    private PayloadDeserializer<OutputT> deserializationSchema;

    private String natsUrl;

    private ConsumerConfiguration cc;

    private String subject;

    public NatsJetstreamSourceBuilder setDeserializationSchema(PayloadDeserializer<OutputT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return  this;
    }

    public NatsJetstreamSourceBuilder setNatsUrl(String natsUrl) {
        this.natsUrl = natsUrl;
        return this;
    }

    public NatsJetstreamSourceBuilder setCc(ConsumerConfiguration cc) {
        this.cc = cc;
        return this;
    }

    public NatsJetstreamSourceBuilder setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public NatsJetstreamSourceBuilder<OutputT> boundedness(Boundedness mode) {
        this.mode = mode;
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
        if (cc == null ) {
            throw new IllegalStateException("Consumer configuration not provided");
        }
        SourceConfiguration sourceConfiguration = new SourceConfiguration(subject, natsUrl, cc);

        return new NatsJetstreamSource<>(deserializationSchema, sourceConfiguration);
    }
}
