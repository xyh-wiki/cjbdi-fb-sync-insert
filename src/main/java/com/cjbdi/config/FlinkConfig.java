package com.cjbdi.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;


/**
 * @Author: XYH
 * @Date: 2021/12/3 1:12 下午
 * @Description: flink 运行环境和参数配置
 */
public class FlinkConfig {

    //运行环境设置
    public static KafkaSource<String> kafkaSource;
    public static KafkaSink<String> kafkaEsSink;

    public void flinkEnv(ParameterTool parameterTool, StreamExecutionEnvironment env) {

        env.getConfig().setGlobalJobParameters(parameterTool);

        env.enableCheckpointing(5 * 60 * 1000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointTimeout(6 * 1000 * 1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //设置可容忍2的检查点失败数，默认值为0表示不允许容忍任何检查点失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(20, Time.seconds(10L)));
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(parameterTool.getRequired("checkpoint.dir")));
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        kafkaSource =  KafkaSource.<String>builder()
                .setBootstrapServers(parameterTool.get("kafka.server"))
                .setTopics(parameterTool.get("kafka.topic"))
                .setGroupId(parameterTool.get("kafka.input.groupId"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

    }
}
