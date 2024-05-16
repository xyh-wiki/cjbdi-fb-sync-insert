package com.cjbdi.processFunction;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cjbdi.utils.YamlUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;

public class UpdateIndexFunction extends ProcessFunction<String, Void> {
    private ParameterTool parameterTool;
    private transient HikariDataSource dataSource;


    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        Class.forName("org.postgresql.Driver");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(parameterTool.get("postgres.index.url"));
        config.setUsername(parameterTool.get("postgres.index.username"));
        config.setPassword(parameterTool.get("postgres.index.password"));
        config.setMaximumPoolSize(1);
        config.setMinimumIdle(0);

        config.setIdleTimeout(600000);  // 设置空闲连接的超时时间为 60 秒
        config.setMaxLifetime(1800000);  // 设置连接的最大生命周期为 30 分钟


        this.dataSource = new HikariDataSource(config);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<Void> out) throws Exception {
        String sourceUrl = parameterTool.get("postgres.url");
        String dbid = parameterTool.get("dbid");
        String schemaName = "index_" + dbid;

        JSONObject jsonObject = JSON.parseObject(value);
        int dataState = jsonObject.getInteger("data_state");
        String tableName = "t_" + jsonObject.getString("schemaName").split("_")[1] + "_index";
        String indexTableName = schemaName + "." + tableName;

        switch (dataState) {
            case 1: // 新增
                String insertSql = String.format(
                        "INSERT INTO %s (c_stm, d_xgsj, c_baah, n_jbfy, create_time, update_time, data_state) " +
                                "VALUES (?, ?, ?, ?, NOW(), NOW(), 1) " +
                                "ON CONFLICT (c_stm) " +
                                "DO UPDATE SET " +
                                "c_stm = EXCLUDED.c_stm, " +
                                "d_xgsj = EXCLUDED.d_xgsj, " +
                                "c_baah = EXCLUDED.c_baah, " +
                                "n_jbfy = EXCLUDED.n_jbfy, " +
                                "update_time = NOW(), " +
                                "data_state = 1", indexTableName);
                try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                    pstmt.setString(1, jsonObject.getString("c_stm"));

                    String d_xgsjValue = jsonObject.containsKey("d_xgsj") ? jsonObject.getString("d_xgsj") : null;

                    Timestamp timestamp = null;

                    if (d_xgsjValue != null && !d_xgsjValue.isEmpty()) {
                        timestamp = Timestamp.valueOf(d_xgsjValue);
                    }

                    pstmt.setTimestamp(2, timestamp);

                    pstmt.setString(3, jsonObject.containsKey("c_baah") ? jsonObject.getString("c_baah") : null);
                    if (jsonObject.containsKey("n_jbfy")) {
                        pstmt.setInt(4, jsonObject.getInteger("n_jbfy"));
                    } else {
                        pstmt.setNull(4, Types.INTEGER);
                    }
                    pstmt.executeUpdate();
                }
                break;
            case 0: // 删除
                String deleteSql = String.format("UPDATE %s SET update_time = NOW(), data_state = 0 WHERE c_stm = ?", indexTableName);
                try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(deleteSql)) {
                    pstmt.setString(1, jsonObject.getString("c_stm"));
                    pstmt.executeUpdate();
                }
                break;
            case 2: // 修改
                String updateSql = String.format(
                        "UPDATE %s SET d_xgsj = ?, c_baah = ?, n_jbfy = ?, update_time = NOW(), data_state = 2 WHERE c_stm = ?",
                        indexTableName);
                try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(updateSql)) {

                    String d_xgsjValue = jsonObject.containsKey("d_xgsj") ? jsonObject.getString("d_xgsj") : null;

                    Timestamp timestamp = null;

                    if (d_xgsjValue != null && !d_xgsjValue.isEmpty()) {
                        timestamp = Timestamp.valueOf(d_xgsjValue);
                    }

                    pstmt.setTimestamp(1, timestamp);
                    pstmt.setString(2, jsonObject.containsKey("c_baah") ? jsonObject.getString("c_baah") : null);

                    if (jsonObject.containsKey("n_jbfy")) {
                        pstmt.setInt(3, jsonObject.getInteger("n_jbfy"));
                    } else {
                        pstmt.setNull(3, Types.INTEGER);
                    }
                    pstmt.setString(4, jsonObject.getString("c_stm"));
                    pstmt.executeUpdate();
                }
                break;
        }
    }
}
