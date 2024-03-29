package com.cjbdi.processFunction;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class PostgreSqlWindowFunction extends ProcessWindowFunction<Tuple2<String, String>, JSONArray, String, TimeWindow> {
    private static final ConcurrentHashMap<String, List<String>> schemaTablesCache = new ConcurrentHashMap<>();
    private static DataSource dataSource;
    private ParameterTool parameterTool;
    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        dataSource = DataSourceSingleton.getDataSource(parameterTool);
    }

    @Override
    public void process(String schema, Context context, Iterable<Tuple2<String, String>> elements, Collector<JSONArray> out) {

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
        if (dataSource == null) {
            log.error("尝试使用未初始化的 dataSource");
            return tables;
        }

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE 't_%';")) {
            stmt.setString(1, schema);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                tables.add(rs.getString("table_name"));
            }
        } catch (Exception e) {
            log.error("schema {} 获取表失败！！{}: " + schema, e);
        }
        return tables;
    }

    private JSONArray executeCStmQuery(String schema, String table, List<String> cStms, Collector<JSONArray> out, ParameterTool parameterTool) {
        ResultSet rs = null;
        try (Connection connection = dataSource.getConnection();

             PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + schema + "." + table + " WHERE c_stm = ANY (?)")) {

            java.sql.Array sqlArray = connection.createArrayOf("text", cStms.toArray(new String[0]));

            stmt.setArray(1, sqlArray);

            rs = stmt.executeQuery();

            String url = parameterTool.get("postgres.url");
            if (rs != null) {
                return resultSetToJsonArray(rs, schema, table, url.substring(url.length() - 2));
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error("Error querying data from table: " + table + " in schema: " + schema, e);
            // 处理异常
            return null;
        } finally {
            // 关闭ResultSet对象
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private static void initDataSource(ParameterTool parameterTool) {
        try {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(parameterTool.get("postgres.url"));
            config.setUsername(parameterTool.get("postgres.username"));
            config.setPassword(parameterTool.get("postgres.password"));
            config.setMaximumPoolSize(3);

            dataSource = new HikariDataSource(config);

            log.info("数据源已初始化。URL: {}", config.getJdbcUrl());


        } catch (Exception e) {
            log.error("dataSource初始化异常！{}", e.getLocalizedMessage());
        }
    }

    public static JSONArray resultSetToJsonArray(ResultSet rs, String schema, String table, String dbid) {

        JSONArray jsonArray = new JSONArray();

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
                //主表
                if (schema.split("_")[1].equals(table.split("_")[1])) {
                    obj.put("tableName", "t_" + schema);
                } else {
                    obj.put("tableName", schema + "_" + table);
                }
                obj.put("c_dt", getCurrentPartition());
                obj.put("create_time", System.currentTimeMillis());
                obj.put("dbid", dbid);

                jsonArray.add(obj);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonArray;
    }

    private static String getCurrentPartition() {
        // 获取当前分区信息
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date());
    }

}