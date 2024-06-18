package io.synadia.flink.source.js;

import io.synadia.flink.source.split.NatsSubjectSplit;

public class NatsSubjectSplitState {

    private NatsSubjectSplit split;

    public NatsSubjectSplitState(NatsSubjectSplit split) {
        this.split = split;
    }

    public NatsSubjectSplit toNatsSubjectSplit() {
        return new NatsSubjectSplit(split.getSubject(),split.getLastConsumedSequenceId());
    }

    public NatsSubjectSplit getSplit() {
        return split;
    }
}
