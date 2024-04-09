package com.cjbdi.job;

import com.cjbdi.processFunction.*;
import com.cjbdi.config.Config;
import com.cjbdi.utils.FileSinkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: LoadFb
 * @Time: 2024/3/28 18:51
 * @Author: XYH
 * @Description: TODO
 */
@Slf4j
public class LoadFb {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        //获取全局参数
        Configuration configuration = new Configuration();

        //flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        //flink 相关配置
        Config flinkConfig = new Config();
        flinkConfig.flinkEnv(parameterTool, env);

        //传递全局参数
        env.getConfig().setGlobalJobParameters(parameterTool);

        //从指定的kafkaSouse中读取数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                        Config.kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "kafka.source")
                .setParallelism(parameterTool.getInt("source.parallelism", 1));

        FileSink<String> fileSink = FileSinkUtils.myFileSink(parameterTool.get("warehouse.path", "hdfs:///data/hive/warehouse/"));

        DataStream<String> processedStream = kafkaSource
                .map(new QueryDatabaseMap(parameterTool)).setParallelism(3)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return !value.isEmpty();
                    }
                });

        processedStream.sinkTo(fileSink);

        // 更新postgres索引表
        kafkaSource.process(new UpdateIndexFunction());

        env.execute("法标增量同步");
    }
}

