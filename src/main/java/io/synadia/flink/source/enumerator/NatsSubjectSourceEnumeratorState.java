// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.source.enumerator;

import io.synadia.flink.source.split.NatsSubjectSplit;
import org.apache.flink.annotation.Internal;

import java.util.Set;

/** The state of Nats source enumerator. */
@Internal
public class NatsSubjectSourceEnumeratorState {
    private final Set<NatsSubjectSplit> unassignedSplits;

    public NatsSubjectSourceEnumeratorState(Set<NatsSubjectSplit> unassignedSplits) {
        this.unassignedSplits = unassignedSplits;
    }

    public Set<NatsSubjectSplit> getUnassignedSplits() {
        return unassignedSplits;
    }
}
