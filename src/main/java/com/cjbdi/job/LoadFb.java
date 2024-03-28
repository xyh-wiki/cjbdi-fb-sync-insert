package com.cjbdi.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cjbdi.bean.SourceBean;
import com.cjbdi.trigger.CountOrTimeTrigger;
import config.FlinkConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @ClassName: LoadFb
 * @Time: 2024/3/28 18:51
 * @Author: XYH
 * @Description: TODO
 */
public class LoadFb {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        //获取全局参数
        Configuration configuration = new Configuration();
        //自定义 jobId
        configuration.setString(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, parameterTool.get("jobId"));

        //flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        //flink 相关配置
        FlinkConfig flinkConfig = new FlinkConfig();
        flinkConfig.flinkEnv(parameterTool, env);

        //传递全局参数
        env.getConfig().setGlobalJobParameters(parameterTool);

        //从指定的kafkaSouse中读取数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                        FlinkConfig.kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "kafka.source")
                .setParallelism(parameterTool.getInt("source.parallelism", 2));


        kafkaSource
                .flatMap(new JSONParser())
                .keyBy(value -> value.getSchemaName()) // 根据schemaName进行分区
                .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5))) // 定义窗口大小
                .trigger(CountOrTimeTrigger.of(500, 5000)); // 使用自定义触发器


        env.execute();
    }

    // 解析JSON数据的FlatMapFunction实现
    public static class JSONParser implements FlatMapFunction<String, SourceBean> {

        @Override
        public void flatMap(String s, Collector<SourceBean> collector) throws Exception {
            SourceBean sourceBean = JSON.parseObject(s, SourceBean.class);
        }
    }
}
