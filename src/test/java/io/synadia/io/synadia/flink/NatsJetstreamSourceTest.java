// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.io.synadia.flink;

import static org.junit.jupiter.api.Assertions.assertTrue;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.SequenceInfo;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.synadia.flink.Utils;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.payload.StringPayloadDeserializer;
import io.synadia.flink.source.js.NatsConsumerConfig;
import io.synadia.flink.source.js.NatsJetstreamSource;
import io.synadia.flink.source.js.NatsJetstreamSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class NatsJetstreamSourceTest extends TestBase{

    @Test
    public void testSource() throws Exception {
        String sourceSubject1 = "test";
        String streamName = "test";
        String consumerName = "Test";

        runInExternalServer(true, (nc, url) -> {

            // publish to the source's subjects
            StreamConfiguration stream = new StreamConfiguration.Builder().name(streamName).subjects(sourceSubject1).build();
            nc.jetStreamManagement().addStream(stream);
            ConsumerConfiguration consumerConfiguration = ConsumerConfiguration.builder()
                            .durable(consumerName).ackPolicy(AckPolicy.All)
                            .filterSubject(sourceSubject1).build();
            nc.jetStreamManagement().addOrUpdateConsumer(streamName, consumerConfiguration);
            nc.jetStream().publish(sourceSubject1, "Hi".getBytes());
            nc.jetStream().publish(sourceSubject1, "Hello".getBytes());

            // --------------------------------------------------------------------------------
            Properties connectionProperties = defaultConnectionProperties(url);
            PayloadDeserializer<String> deserializer = new WriteData();
            NatsConsumerConfig consumerConfig = new NatsConsumerConfig.Builder().withConsumerName(consumerName).
                    withBatchSize(5).withStreamName(streamName).build();
            NatsJetstreamSourceBuilder<String> builder = new NatsJetstreamSourceBuilder<String>()
                    .setDeserializationSchema(deserializer)
                    .setCc(consumerConfiguration)
                    .setNatsUrl("localhost:4222")
                    .setSubject(sourceSubject1);

            NatsJetstreamSource<String> natsSource = builder.build();
            StreamExecutionEnvironment env = getStreamExecutionEnvironment();
           // env.enableCheckpointing(10000L, CheckpointingMode.AT_LEAST_ONCE);
           // env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
            env.getCheckpointConfig().setCheckpointInterval(10000L);
           // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1 * 60 * 1000);
            //env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            //env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000);
            DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(),"nats-source-input");
            ds.map(String::toUpperCase);//To Avoid Sink Dependency
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));

            env.executeAsync("nats-flink");
            Thread.sleep(500000);
            env.close();
            SequenceInfo sequenceInfo = nc.jetStream().getConsumerContext(sourceSubject1,consumerName).getConsumerInfo().getDelivered();
            assertTrue(sequenceInfo.getStreamSequence()>0);
        });
    }
}
class WriteData implements PayloadDeserializer<String> {


    @Override
    public String getObject(String subject, byte[] input, Headers headers) {
        String data = new String(input);
        System.out.println(data);
        return data;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Utils.getTypeInformation(String.class);
    }
}
