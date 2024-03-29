package com.cjbdi.job;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cjbdi.bean.SourceBean;
import com.cjbdi.processFunction.PostgreSqlWindowFunction;
import com.cjbdi.trigger.CountOrTimeTrigger;
import com.cjbdi.config.Config;
import com.cjbdi.utils.FileSinkUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

//        initDataSource(parameterTool);

        //从指定的kafkaSouse中读取数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                        Config.kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "kafka.source")
                .setParallelism(parameterTool.getInt("source.parallelism", 2));

//        kafkaSource.print("kafka-source");

        FileSink<String> fileSink = FileSinkUtils.myFileSink(parameterTool.get("warehouse.path", "hdfs:///data/hive/warehouse/"));

        WindowedStream<Tuple2<String, String>, String, TimeWindow> windowedStream = kafkaSource
                .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, String>> out) {
                        SourceBean sourceBean = JSONObject.parseObject(value, SourceBean.class);
                        String schemaName = sourceBean.getSchemaName();
                        String stm = sourceBean.getC_stm();
                        out.collect(new Tuple2<>(schemaName, stm));
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, String>, String>() {
                    @Override
                    public String getKey(Tuple2<String, String> value) {
                        return value.f0; // 根据 schema 分组
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 窗口大小示例
                .trigger(CountOrTimeTrigger.of(500, 5000));

        SingleOutputStreamOperator<String> streamOperator = windowedStream
                .process(new PostgreSqlWindowFunction()).setParallelism(3)
                .flatMap(new FlatMapFunction<JSONArray, String>() {
                    @Override
                    public void flatMap(JSONArray objects, Collector<String> collector) throws Exception {
                        for (int i = 0; i < objects.size(); i++) {
                            collector.collect(objects.getJSONObject(i).toJSONString());
                        }
                    }
                });

        streamOperator.sinkTo(fileSink);

        env.execute("法标同步");
    }

}