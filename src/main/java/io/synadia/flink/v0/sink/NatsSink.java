// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.sink;

import io.nats.client.NUID;
import io.synadia.flink.v0.payload.PayloadSerializer;
import io.synadia.flink.v0.sink.writer.NatsSinkWriter;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.util.List;

/**
 * Flink Sink to publish data to one or more NATS subjects
 * @param <InputT> the type of object from the source to convert for publishing
 */
public class NatsSink<InputT> implements Sink<InputT> {
    protected final String id;
    protected final List<String> subjects;
    protected final PayloadSerializer<InputT> payloadSerializer;
    protected final ConnectionFactory connectionFactory;

    protected NatsSink(List<String> subjects,
             PayloadSerializer<InputT> payloadSerializer,
             ConnectionFactory connectionFactory)
    {
        id = NUID.nextGlobal().substring(0, 4).toUpperCase();
        this.subjects = subjects;
        this.payloadSerializer = payloadSerializer;
        this.connectionFactory = connectionFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SinkWriter<InputT> createWriter(InitContext context) throws IOException {
        return new NatsSinkWriter<>(id, subjects, payloadSerializer, connectionFactory, context);
    }

    @Override
    public String toString() {
        return "NatsSink{" +
            "id='" + id + '\'' +
            ", subjects=" + subjects +
            ", payloadSerializer=" + payloadSerializer.getClass().getCanonicalName() +
            ", connectionFactory=" + connectionFactory +
            '}';
    }
}
