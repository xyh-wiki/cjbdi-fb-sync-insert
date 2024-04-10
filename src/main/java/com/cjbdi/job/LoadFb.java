package com.cjbdi.job;

import com.alibaba.fastjson.JSONObject;
import com.cjbdi.processFunction.*;
import com.cjbdi.config.Config;
import com.cjbdi.utils.FileSinkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: LoadFb
 * @Time: 2024/3/28 18:51
 * @Author: XYH
 * @Description: TODO
 */
@Slf4j
public class LoadFb {
    private static final OutputTag<String> exceptionDataStream = new OutputTag<String>("exception-data") {
    };

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        // 创建测流输出标签

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
                        "kafka 读取数据")
                .setParallelism(parameterTool.getInt("source.parallelism", 1));

        FileSink<String> fileSink = FileSinkUtils.myFileSink(parameterTool.get("warehouse.path", "hdfs:///data/hive/warehouse/"));

        SingleOutputStreamOperator<String> queryStream = kafkaSource
                .process(new QueryDatabaseFunction(parameterTool, exceptionDataStream))
                .setParallelism(3)
                .name("法标库查询拉取数据");

        SideOutputDataStream<String> exceptionStream = queryStream.getSideOutput(exceptionDataStream);

        queryStream.sinkTo(fileSink).name("hdfs写入数据");

        exceptionStream.process(new UpdateIndexFunction()).name("更新索引表");

        env.execute("法标增量同步-" + parameterTool.get("dbid"));
    }
}

