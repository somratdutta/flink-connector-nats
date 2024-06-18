package io.synadia.flink.source;

import java.time.Duration;

public class SourceConfiguration {

    private static final long serialVersionUID = 8488507275800787580L;

    private  int messageQueueCapacity;
    private  boolean enableAutoAcknowledgeMessage;
    private  long autoCommitCursorInterval;
    private  int fetchOneMessageTime;
    private  Duration maxFetchTime;
    private  int maxFetchRecords;
    private  String consumerName;


    /**
     * Creates a new UnmodifiableConfiguration, which holds a copy of the given configuration that
     * cannot be altered.
     *
     * @param config The configuration with the original contents.
     */
    //TODO Pick from client provided configuration
    public SourceConfiguration() {
        messageQueueCapacity = 100;
        maxFetchTime = Duration.ofMillis(5);
        maxFetchRecords = 100;
        consumerName = "Test";
        fetchOneMessageTime = 1000;
    }

    public int getMessageQueueCapacity() {
        return messageQueueCapacity;
    }

    public boolean isEnableAutoAcknowledgeMessage() {
        return enableAutoAcknowledgeMessage;
    }

    public long getAutoCommitCursorInterval() {
        return autoCommitCursorInterval;
    }

    public int getFetchOneMessageTime() {
        return fetchOneMessageTime;
    }

    public Duration getMaxFetchTime() {
        return maxFetchTime;
    }

    public int getMaxFetchRecords() {
        return maxFetchRecords;
    }

    public String getConsumerName() {
        return consumerName;
    }
}
