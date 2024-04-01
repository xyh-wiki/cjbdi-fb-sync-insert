package com.cjbdi.job;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cjbdi.bean.SourceBean;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

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

//        streamOperator.sinkTo(fileSink);
@Slf4j
public class QueryDatabaseMap extends RichMapFunction<String, String> {

    private static final ConcurrentHashMap<String, List<String>> schemaTablesCache = new ConcurrentHashMap<>();

    private transient DataSource dataSource;
    private ParameterTool parameterTool;

    public QueryDatabaseMap(ParameterTool params) {
        this.parameterTool = params;
    }

    @Override
    public void open(Configuration parameters) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(parameterTool.get("postgres.url"));
        config.setUsername(parameterTool.get("postgres.username"));
        config.setPassword(parameterTool.get("postgres.password"));
        config.setMaximumPoolSize(1); // 根据实际情况调整连接池大小

        this.dataSource = new HikariDataSource(config);
    }

    @Override
    public String map(String value) {
        return queryDatabase(value);
    }

    private String queryDatabase(String value) {
        SourceBean sourceBean = JSONObject.parseObject(value, SourceBean.class);

        String cStm = sourceBean.getC_stm();
        String schemaName = sourceBean.getSchemaName();
        int dataState = sourceBean.getData_state();

        // 从缓存中获取或查询数据库以填充schema对应的表名列表
        List<String> tables = schemaTablesCache.computeIfAbsent(schemaName, this::fetchTablesForSchema);

        JSONObject result = new JSONObject();
        // 遍历表名，对每个表执行查询
        for (String table : tables) {
            String query = "SELECT * FROM " + schemaName + "." + table + " WHERE c_stm = ?";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(query)) {

                stmt.setString(1, cStm);
                ResultSet rs = stmt.executeQuery();

                // 处理查询结果
                if (rs != null) {
                    // 此处应实现resultSetToJsonArray，转换结果集为JSONObject或JSONArray
                    JSONObject jsonObject = resultSetToJsonArray(rs, schemaName, table, parameterTool.get("postgres.url").substring(parameterTool.get("postgres.url").length() - 2), dataState);
                    // 这里简化处理，假设只关心第一个查询结果
                    return jsonObject.toJSONString();
                }
            } catch (Exception e) {
                log.error("数据库查询失败", e);
            }
        }
        return null;
    }

    public static JSONObject resultSetToJsonArray(ResultSet rs, String schema, String table, String dbid, int dataState) {

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
                obj.put("data_state", dataState);

                return obj;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String getCurrentPartition() {
        // 获取当前分区信息
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date());
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
}
