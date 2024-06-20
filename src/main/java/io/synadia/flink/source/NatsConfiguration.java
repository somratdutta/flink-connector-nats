package io.synadia.flink.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.UnmodifiableConfiguration;

public class NatsConfiguration extends UnmodifiableConfiguration {
    /**
     * Creates a new UnmodifiableConfiguration, which holds a copy of the given configuration that
     * cannot be altered.
     *
     * @param config The configuration with the original contents.
     */
    public NatsConfiguration(Configuration config) {
        super(config);
    }
}
