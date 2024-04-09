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
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
        config.setMaximumPoolSize(1);

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
                ResultSet rs = null;
                try {
                    rs = stmt.executeQuery();
                } catch (SQLException e) {
                    log.error("===数据库查询失败=== {}", e.getLocalizedMessage());
                }

                // 处理查询结果
                if (rs.next() && rs != null) {

                    try {
                        JSONObject jsonObject = resultSetToJsonArray(rs, schemaName, table, parameterTool.get("dbid"), dataState);
                        return jsonObject.toJSONString();
                    } catch (Exception e) {
                        log.error("===查询结果 json 处理失败=== {}", e.getLocalizedMessage());
                    }
                }
            } catch (Exception e) {
                log.error("===数据库连接异常=== {}", e);
            }
        }
        return "";
    }

    public static JSONObject resultSetToJsonArray(ResultSet rs, String schema, String table, String dbid, int dataState) {

        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            JSONObject obj = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rsmd.getColumnLabel(i);
                String columnValue = rs.getString(i);
                obj.put(columnName, columnValue);

                obj.put("tableName", schema + "_" + table);
                obj.put("c_dt", getCurrentPartition());
                obj.put("update_time", LocalDateTime.now());
                obj.put("dbid", dbid);
                obj.put("lsn", null);
                obj.put("data_state", dataState);

                return obj;
            }

        } catch (Exception e) {
            log.error("查询结果处理失败!!, {}", e.getLocalizedMessage());
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
