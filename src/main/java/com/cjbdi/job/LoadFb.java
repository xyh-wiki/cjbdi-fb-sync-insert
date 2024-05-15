package com.cjbdi.job;

import com.cjbdi.config.YamlManager;
import com.cjbdi.processFunction.*;
import com.cjbdi.config.JobConfig;
import com.cjbdi.utils.FileSinkUtils;
import com.cjbdi.utils.YamlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.Map;
import java.util.Properties;

/**
 * @ClassName: LoadFb
 * @Time: 2024/3/28 18:51
 * @Author: XYH
 * @Description: 法标库增量同步, 数据库查询， hdfs写入
 */
@Slf4j
public class LoadFb {
    private static final OutputTag<String> INDEX_TAG = new OutputTag<String>("indexTag") {};

    public static void main(String[] args) throws Exception {
        // 加载配置文件
//        Map<String, Object> yamlConfig = YamlUtils.loadYaml(args[0]);

        Map<String, Object> yamlConfig = YamlUtils.loadYamlFromEnv("CONFIG_PATH");

        YamlManager.setConfiguration(yamlConfig);

        // 获取 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 Hadoop 用户名
        JobConfig.setHadoopUsername();

        // 配置 Flink 环境
        JobConfig.configureFlinkEnvironment(env);

        // 从 Kafka 中读取数据
        DataStream<String> kafkaSource = readDataFromKafka(env);

        // 数据处理及写入到 HDFS
        processAndWriteToHDFS(kafkaSource, yamlConfig);

        // 执行任务
        executeJob(env);
    }

    private static void loadConfiguration(String yamlPath) {
        Map<String, Object> yamlConfig = YamlUtils.loadYaml(yamlPath);
        YamlManager.setConfiguration(yamlConfig);
    }

    private static DataStream<String> readDataFromKafka(StreamExecutionEnvironment env) {
        Properties kafkaProperties = JobConfig.configureKafka();
        return env.fromSource(
                JobConfig.createKafkaSource(kafkaProperties),
                WatermarkStrategy.noWatermarks(),
                "kafka 读取数据"
        ).setParallelism(YamlManager.getJobSourceParallelism(1));
    }

    private static void processAndWriteToHDFS(DataStream<String> kafkaSource, Map<String, Object> yamlPath) {
        FileSink<String> fileSink = FileSinkUtils.myFileSink(YamlManager.getWarehousePath("hdfs:///data/hive/warehouse/"));

        SingleOutputStreamOperator<String> queryStream = kafkaSource
                .process(new QueryDatabaseFunction(INDEX_TAG, yamlPath))
                .setParallelism(YamlManager.getPostgresSourceConnections(3))
                .name("法标库查询拉取数据");

        SideOutputDataStream<String> indexStream = queryStream.getSideOutput(INDEX_TAG);

        queryStream.sinkTo(fileSink).name("hdfs写入数据").setParallelism(YamlManager.getJobHdfsSinkParallelism(1));

        indexStream.process(new UpdateIndexFunction()).name("更新索引表").setParallelism(YamlManager.getJobIndexSinkParallelism(1));
    }

    private static void executeJob(StreamExecutionEnvironment env) throws Exception {
        env.execute("法标增量同步-" + YamlManager.getPostgresSourceDbId());
    }
}

