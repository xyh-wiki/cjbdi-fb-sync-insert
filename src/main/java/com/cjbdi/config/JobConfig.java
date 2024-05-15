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

import java.util.Properties;

/**
 * @Author: XYH
 * @Date: 2021/12/3 1:12 下午
 * @Description: Flink 运行环境和参数配置
 */
public class JobConfig {

    // 设置 Flink 运行环境
    public static void configureFlinkEnvironment(StreamExecutionEnvironment env) {
        env.enableCheckpointing(5 * 60 * 1000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointTimeout(6 * 1000 * 1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10L)));
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(YamlManager.getCheckpointDir()));
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    // Kafka 配置
    public static Properties configureKafka() {
        Properties properties = new Properties();
        properties.setProperty("request.timeout.ms", "214748364");
        properties.setProperty("metadata.fetch.timeout.ms", "214748364");
        properties.setProperty("max.poll.records", "5000");
        properties.setProperty("retries", "20");
        properties.setProperty("linger.ms", "300");
        properties.setProperty("log.level", "warn");
        return properties;
    }

    // 创建 Kafka Source
    public static KafkaSource<String> createKafkaSource(Properties properties) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(YamlManager.getKafkaServer())
                .setTopics(YamlManager.getKafkaTopic())
                .setGroupId(YamlManager.getKafkaGroupId())
                .setProperties(properties)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static void setHadoopUsername() {
        System.setProperty("HADOOP_USER_NAME", YamlManager.getWarehouseUsername("root"));
    }
}
