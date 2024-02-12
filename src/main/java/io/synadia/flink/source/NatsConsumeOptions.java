// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;
import java.io.Serializable;

public class NatsConsumeOptions implements Serializable {

    private final String consumerName;
    private final String streamName;
    private final int batchSize;


    private NatsConsumeOptions(Builder builder) {
        this.consumerName = builder.consumerName;
        this.batchSize = builder.batchSize;
        this.streamName = builder.streamName;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public String getStreamName() {
        return streamName;
    }


    public int getBatchSize() {
        return batchSize;
    }

    public static class Builder {
        private String consumerName;
        private int batchSize;
        private String streamName;

        public Builder() {
        }

        public Builder consumer(String consumerName) {
            this.consumerName = consumerName;
            return this;
        }

        public Builder stream(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }


<<<<<<< HEAD:src/main/java/io/synadia/flink/source/js/NatsConsumerConfig.java
        public NatsConsumerConfig build() {
            return new NatsConsumerConfig(this);
=======

        public NatsConsumeOptions build() {
            return new NatsConsumeOptions(this);
>>>>>>> c29606f (Cosmetic changes to classes as per Scott's request):src/main/java/io/synadia/flink/source/NatsConsumeOptions.java
        }
    }
}
