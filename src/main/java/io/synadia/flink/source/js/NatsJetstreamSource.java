// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.js;

import io.nats.client.NUID;
import io.synadia.flink.Utils;
import io.synadia.flink.common.ConnectionFactory;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.SourceConfiguration;
import io.synadia.flink.source.enumerator.NatsSourceEnumerator;
import io.synadia.flink.source.split.NatsSubjectCheckpointSerializer;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.source.split.NatsSubjectSplitSerializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NatsJetstreamSource<OutputT> implements Source<OutputT, NatsSubjectSplit, Collection<NatsSubjectSplit>>, ResultTypeQueryable<OutputT> {

    private final PayloadDeserializer<OutputT> deserializationSchema;
    private static final Logger LOG = LoggerFactory.getLogger(NatsJetstreamSource.class);
    private SourceConfiguration sourceConfiguration;


    // Package-private constructor to ensure usage of the Builder for object creation
    NatsJetstreamSource(PayloadDeserializer<OutputT> deserializationSchema, SourceConfiguration sourceConfiguration) {
        this.deserializationSchema = deserializationSchema;
        this.sourceConfiguration = sourceConfiguration;
    }

    @Override
    public TypeInformation<OutputT> getProducedType() {
        return this.deserializationSchema.getProducedType();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<NatsSubjectSplit, Collection<NatsSubjectSplit>> createEnumerator(
            SplitEnumeratorContext<NatsSubjectSplit> enumContext) throws Exception {
        List<NatsSubjectSplit> list = new ArrayList<>();
        list.add(new NatsSubjectSplit(sourceConfiguration.getSubjectName()));
        return restoreEnumerator(enumContext, list);
    }

    @Override
    public SplitEnumerator<NatsSubjectSplit, Collection<NatsSubjectSplit>> restoreEnumerator(
            SplitEnumeratorContext<NatsSubjectSplit> enumContext, Collection<NatsSubjectSplit> checkpoint)
            throws Exception {
        return new NatsSourceEnumerator(NUID.nextGlobal(), enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<NatsSubjectSplit> getSplitSerializer() {
        return new NatsSubjectSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<NatsSubjectSplit>> getEnumeratorCheckpointSerializer() {
        return new NatsSubjectCheckpointSerializer();
    }

    @Override
    public SourceReader<OutputT, NatsSubjectSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return NatsJetstreamSourceReader.create(sourceConfiguration, deserializationSchema, readerContext);
    }

}
