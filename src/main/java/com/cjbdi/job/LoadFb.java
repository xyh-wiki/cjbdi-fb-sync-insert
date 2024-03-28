package com.cjbdi.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cjbdi.bean.SourceBean;
import com.cjbdi.trigger.CountOrTimeTrigger;
import com.cjbdi.config.FlinkConfig;
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
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.sql.DataSource;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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

    private static DataSource dataSource;
    private static final ConcurrentHashMap<String, List<String>> schemaTablesCache = new ConcurrentHashMap<>();


    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        initDataSource();

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        //获取全局参数
        Configuration configuration = new Configuration();

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


        SingleOutputStreamOperator<String> process = kafkaSource
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
                .trigger(CountOrTimeTrigger.of(500, 5000))
                .process(new PostgreSqlWindowFunction());

        FileSink<String> fileSink = FileSinkUtils.myFileSink(parameterTool.get("warehouse.path", "hdfs:///data/hive/warehouse/"));


        process.print("结果打印！！");

//        process.sinkTo(fileSink);

        env.execute("法标同步");
    }

    private static void initDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://192.168.50.37:5432/fb15_00");
        config.setUsername("postgres");
        config.setPassword("121019");
        config.setMaximumPoolSize(3);

        dataSource = new HikariDataSource(config);
    }


    public static class PostgreSqlWindowFunction extends ProcessWindowFunction<Tuple2<String, String>, String, String, TimeWindow> {

        private ParameterTool parameterTool;

        @Override
        public void open(Configuration parameters) throws Exception {
            parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        }

        @Override
        public void process(String schema, Context context, Iterable<Tuple2<String, String>> elements, Collector<String> out) {
            // 尝试获取该 schema 下的表名列表，如果不存在，则查询数据库
            schemaTablesCache.computeIfAbsent(schema, k -> fetchTablesForSchema(schema));

            List<String> tables = schemaTablesCache.get(schema);
            List<String> cStms = new ArrayList<>();
            for (Tuple2<String, String> element : elements) {
                cStms.add(element.f1);
            }

            // 对于 schema 下的每个表，执行 c_stm 查询
            tables.forEach(table -> out.collect(executeCStmQuery(schema, table, cStms, out, parameterTool)));
        }

        private List<String> fetchTablesForSchema(String schema) {
            List<String> tables = new ArrayList<>();
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement stmt = connection.prepareStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = ?")) {
                stmt.setString(1, schema);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    tables.add(rs.getString("table_name"));
                }
            } catch (Exception e) {
                log.error("Error querying tables in schema: " + schema, e);
            }
            return tables;
        }

        private String executeCStmQuery(String schema, String table, List<String> cStms, Collector<String> out, ParameterTool parameterTool) {
            ResultSet rs = null;
            try (Connection connection = dataSource.getConnection();

                 PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + schema + "." + table + " WHERE c_stm = ANY (?)")) {

                java.sql.Array sqlArray = connection.createArrayOf("text", cStms.toArray(new String[0]));
                stmt.setArray(1, sqlArray);
                rs = stmt.executeQuery();
                if (rs != null) {
                    return resultSetToJsonArray(rs);
                } else {
                    // 处理ResultSet为null的情况
                    // 这里可以返回空字符串或者其他默认值
                    return "";
                }
            } catch (Exception e) {
                log.error("Error querying data from table: " + table + " in schema: " + schema, e);
                // 处理异常，返回空字符串或者其他默认值
                return "";
            } finally {
                // 在这里关闭ResultSet对象
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


        public static String resultSetToJsonArray(ResultSet rs) {

        JSONArray jsonArray = new JSONArray();

        String jsonStr = null;
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            while (rs.next()) {
                JSONObject obj = new JSONObject();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rsmd.getColumnLabel(i);
                    String columnValue = rs.getString(i);
                    obj.put(columnName, columnValue);
                }
                jsonArray.add(obj);
            }

            jsonStr = jsonArray.toJSONString();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonStr;
    }

    public static void writeJsonToHdfs(String jsonStr, String hdfsFilePath) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.50.37:8020");
        try {
            FileSystem fs = FileSystem.get(conf);
            Path file = new Path(hdfsFilePath);

            // 创建一个输出流
            OutputStream os = fs.create(file);

            PrintWriter writer = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(os), "UTF-8"));

            // 写入JSON字符串
            writer.write(jsonStr);

            // 关闭输出流
            writer.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
}