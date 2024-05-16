package com.cjbdi.job;

import com.cjbdi.processFunction.*;
import com.cjbdi.config.JobConfig;
import com.cjbdi.utils.FileSinkUtils;
import com.cjbdi.utils.YamlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
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
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        // 设置 Hadoop 用户名
        JobConfig.setHadoopUsername(parameterTool);

        // 配置 Flink 环境
        JobConfig.configureFlinkEnvironment(env, parameterTool);

        // 从 Kafka 中读取数据
        DataStream<String> kafkaSource = readDataFromKafka(env, parameterTool);

        // 数据处理及写入到 HDFS
        processAndWriteToHDFS(kafkaSource, parameterTool);

        // 执行任务
        executeJob(env, parameterTool);
    }

    private static DataStream<String> readDataFromKafka(StreamExecutionEnvironment env, ParameterTool parameterTool) {
        Properties kafkaProperties = JobConfig.configureKafka();
        return env.fromSource(
                JobConfig.createKafkaSource(kafkaProperties, parameterTool),
                WatermarkStrategy.noWatermarks(),
                "kafka 读取数据"
        ).setParallelism(parameterTool.getInt("kafka.source.parallelism", 1));
    }

    private static void processAndWriteToHDFS(DataStream<String> kafkaSource, ParameterTool parameterTool) {
        FileSink<String> fileSink = FileSinkUtils.myFileSink(parameterTool.get("warehouse.path", "hdfs:///data/hive/warehouse"));

        SingleOutputStreamOperator<String> queryStream = kafkaSource
                .process(new QueryDatabaseFunction(INDEX_TAG))
                .setParallelism(parameterTool.getInt("database.connections", 3))
                .name("法标库查询拉取数据");

        SideOutputDataStream<String> indexStream = queryStream.getSideOutput(INDEX_TAG);

        queryStream.sinkTo(fileSink).name("hdfs写入数据").setParallelism(parameterTool.getInt("hdfs.sink.parallelism", 2));

        indexStream.process(new UpdateIndexFunction()).name("更新索引表").setParallelism(parameterTool.getInt("index.sink.parallelism", 1));
    }

    private static void executeJob(StreamExecutionEnvironment env, ParameterTool parameterTool) throws Exception {
        env.execute("法标增量同步-" + parameterTool.get("dbid"));
    }
}
