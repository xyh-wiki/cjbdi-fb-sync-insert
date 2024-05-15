package com.cjbdi.processFunction;

import com.alibaba.fastjson.JSONObject;
import com.cjbdi.bean.SourceBean;
import com.cjbdi.config.YamlManager;
import com.cjbdi.utils.YamlUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.sql.DataSource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName: QueryDatabaseFunction
 * @Time: 2024/4/9 19:34
 * @Author: XYH
 * @Description: TODO
 */
@Slf4j
public class QueryDatabaseFunction extends ProcessFunction<String, String> {

    private static final ConcurrentHashMap<String, List<String>> schemaTablesCache = new ConcurrentHashMap<>();

    private final OutputTag<String> mainTableTag;

    private transient HikariDataSource dataSource;
    private Set<String> uniqueSchemaTableNames;
    private static String dbId;
    private static String indexUrl;
    private static String indexUsername;
    private static String indexPassword;
    private final Map<String, Object> yamlConfig;


    public QueryDatabaseFunction(OutputTag<String> indexTag, Map<String, Object> yamlConfig) {
        this.mainTableTag = indexTag;
        this.yamlConfig = yamlConfig;
    }

    @Override
    public void open(Configuration parameters) {

        YamlManager.setConfiguration(yamlConfig);
        dbId = YamlManager.getPostgresSourceDbId();
        indexUrl = YamlManager.getPostgresIndexUrl();
        indexUsername = YamlManager.getPostgresIndexUsername();
        indexPassword = YamlManager.getPostgresIndexPassword();

        System.out.println("dbId>> " + dbId);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(YamlManager.getPostgresSourceUrl());
        config.setUsername(YamlManager.getPostgresSourceUsername());
        config.setPassword(YamlManager.getPostgresSourcePassword());
        config.setMaximumPoolSize(1);
        config.setMinimumIdle(0);

        config.setIdleTimeout(600000);  // 设置空闲连接的超时时间为 60 秒
        config.setMaxLifetime(1800000);  // 设置连接的最大生命周期为 30 分钟


        this.dataSource = new HikariDataSource(config);

        // 获取非规范从表表名和schema名
        this.uniqueSchemaTableNames = fetchUniqueSchemaTableNames();

    }

    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            dataSource.close();
        }
        super.close();
    }

    @Override
    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {

        String currentPartition = getCurrentPartition();

        SourceBean sourceBean = JSONObject.parseObject(value, SourceBean.class);

        String cStm = sourceBean.getC_stm();
        String schemaName = sourceBean.getSchemaName();
        int dataState = sourceBean.getData_state();

        if (dataState == 0) {
            JSONObject obj = JSONObject.parseObject(value);
            String name = obj.getString("schemaName");
            obj.put("tableName", name + "_t_" + name.split("_")[1]);
            obj.put("dt", currentPartition);
            obj.put("update_time", LocalDateTime.now());
            obj.put("dbid", dbId);
            obj.put("lsn", null);
            obj.put("data_state", dataState);
            ctx.output(mainTableTag, value);
            out.collect(obj.toJSONString());
        } else {
            // 从缓存中获取或查询数据库以填充schema对应的表名列表
            List<String> tables = schemaTablesCache.computeIfAbsent(schemaName, this::fetchTablesForSchema);

            // 遍历表名，对每个表执行查询
            for (String table : tables) {
                String query;

                // 主表或者非规范从表（从表关联键名称不是c_stm_schema）
                if (uniqueSchemaTableNames.contains(schemaName + "." + table)) {
                    query = "SELECT * FROM " + schemaName + "." + table + " WHERE c_stm = ?";
                } else {
                    query = "SELECT * FROM " + schemaName + "." + table + " WHERE c_stm_" + schemaName.split("_")[1] + "  = ?";
                }

                try (Connection conn = dataSource.getConnection();
                     PreparedStatement stmt = conn.prepareStatement(query)) {

                    stmt.setString(1, cStm);
                    ResultSet rs = null;
                    try {
                        rs = stmt.executeQuery();
                    } catch (SQLException e) {
                        log.error("数据库查询失败>> {}", e.getLocalizedMessage());
                        // 插入异常日志到数据库表
                        try {
                            Connection indexConn = DriverManager.getConnection(indexUrl, indexUsername, indexPassword);
                            PreparedStatement pstmt = conn.prepareStatement("INSERT INTO index_log.t_log (dbId, tableName, c_stm, log_message, job_mode, c_dt, create_time) VALUES (?, ?, ?, ?, 'increment', ?, now())");
                            pstmt.setString(1, dbId);
                            pstmt.setString(2, schemaName + "." + table);
                            pstmt.setString(3, cStm);
                            pstmt.setString(4, "法标库查询异常>> " + e.getMessage());
                            pstmt.setString(5, currentPartition);
                            pstmt.executeUpdate();
                            pstmt.close();
                            indexConn.close();
                        } catch (SQLException ex) {
                            log.error("插入日志表时发生数据库错误", ex);
                        }
                    }

                    // 处理查询结果
                    while (rs.next()) {
                        try {

                            JSONObject jsonObject = resultSetToJsonArray(rs, schemaName, table, dbId, dataState);

                            if (schemaName.split("_")[1].equals(table.split("_")[1])) {
                                ctx.output(mainTableTag, value);
                            }
                            out.collect(jsonObject.toJSONString());
                        } catch (Exception e) {
                            log.error("查询结果 json 处理失败>> {}", e.getLocalizedMessage());
                            try {
                                Connection indexConn = DriverManager.getConnection(indexUrl, indexUsername, indexPassword);
                                PreparedStatement pstmt = conn.prepareStatement("INSERT INTO index_log.t_log (dbId, tableName, c_stm, log_message, job_mode, c_dt, create_time) VALUES (?, ?, ?, ?, 'increment', ?, now())");
                                pstmt.setString(1, dbId);
                                pstmt.setString(2, schemaName + "." + table);
                                pstmt.setString(3, cStm);
                                pstmt.setString(4, "查询结果 json 处理失败>> " + e.getMessage());
                                pstmt.setString(5, currentPartition);
                                pstmt.executeUpdate();
                                pstmt.close();
                                indexConn.close();
                            } catch (SQLException ex) {
                                log.error("插入日志表时发生数据库错误>> {}", ex.getLocalizedMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("法标库连接异常>> {}", e.getLocalizedMessage());
                    try {
                        Connection indexConn = DriverManager.getConnection(indexUrl, indexUsername, indexPassword);
                        PreparedStatement pstmt = indexConn.prepareStatement("INSERT INTO index_log.t_log (dbId, tableName, c_stm, log_message, job_mode, c_dt, create_time) VALUES (?, ?, ?, ?, 'increment', ?, now())");
                        pstmt.setString(1, dbId);
                        pstmt.setString(2, schemaName + "." + table);
                        pstmt.setString(3, cStm);
                        pstmt.setString(4, "法标库连接异常>> " + e.getMessage());
                        pstmt.setString(5, currentPartition);
                        pstmt.executeUpdate();
                        pstmt.close();
                        indexConn.close();
                    } catch (SQLException ex) {
                        log.error("插入日志表时发生数据库错误>> {}", ex.getLocalizedMessage());
                    }
                }
            }
        }
    }

    public static JSONObject resultSetToJsonArray(ResultSet rs, String schema, String table, String dbId, int dataState) throws SQLException {

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        JSONObject obj = new JSONObject();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = rsmd.getColumnLabel(i);
            String columnValue = rs.getString(i);

            if (("6".equals(String.valueOf(dbId)) || "22".equals(String.valueOf(dbId))) && schema.equals("db_xzys") && table.equals("t_xzys") && "n_jbfymc".equals(columnName) && columnValue != null) {
                obj.put("c_jbfymc", columnValue);
            }

            obj.put(columnName, columnValue);
            obj.put("tableName", schema + "_" + table);
            obj.put("dt", getCurrentPartition());
            obj.put("update_time", LocalDateTime.now());
            obj.put("dbid", dbId);
            obj.put("lsn", null);
            obj.put("data_state", 1);
        }
        return obj;
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
            log.error("schema {} 获取表失败!!>> {} ", schema, e);

            try {
                Connection indexConn = DriverManager.getConnection(indexUrl, indexUsername, indexPassword);
                PreparedStatement pstmt = indexConn.prepareStatement("INSERT INTO index_log.t_log (dbId, tableName, c_stm, log_message, job_mode, c_dt, create_time) VALUES (?, ?, ?, ?, 'increment', ?, now())");
                pstmt.setString(1, dbId);
                pstmt.setString(2, schema);
                pstmt.setString(3, null);
                pstmt.setString(4, "schema 获取表失败!!>> " + e.getMessage());
                pstmt.setString(5, getCurrentPartition());
                pstmt.executeUpdate();
                pstmt.close();
                indexConn.close();
            } catch (SQLException ex) {
                log.error("插入日志表时发生数据库错误>> {}", ex.getLocalizedMessage());
            }
        }
        return tables;
    }

    // 筛选从表中关联键不为 c_stm_schema 的从表
    private Set<String> fetchUniqueSchemaTableNames() {
        Set<String> schemaTableNames = new HashSet<>();

        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {

            ResultSet rs = stmt.executeQuery(
                    "SELECT table_schema, table_name FROM information_schema.columns " +
                            "WHERE column_name LIKE 'c_stm%' " +
                            "GROUP BY table_schema, table_name " +
                            "HAVING COUNT(*) = 1");

            while (rs.next()) {
                String schemaName = rs.getString("table_schema");
                String tableName = rs.getString("table_name");
                String schemaTableName = schemaName + "." + tableName;
                schemaTableNames.add(schemaTableName);
            }
        } catch (SQLException e) {
            log.error("特殊 schema 查询失败>> {}", e.getLocalizedMessage());

            try {
                Connection indexConn = DriverManager.getConnection(indexUrl, indexUsername, indexPassword);
                PreparedStatement pstmt = indexConn.prepareStatement("INSERT INTO index_log.t_log (dbId, tableName, c_stm, log_message, job_mode, c_dt, create_time) VALUES (?, ?, ?, ?, 'increment', ?, now())");
                pstmt.setString(1, dbId);
                pstmt.setString(2, null);
                pstmt.setString(3, null);
                pstmt.setString(4, "特殊 schema 查询失败>> " + e.getLocalizedMessage());
                pstmt.setString(5, getCurrentPartition());
                pstmt.executeUpdate();
                pstmt.close();
                indexConn.close();
            } catch (SQLException ex) {
                log.error("插入日志表时发生数据库错误>> {}", ex.getLocalizedMessage());
            }
        }

        return schemaTableNames;
    }

}
