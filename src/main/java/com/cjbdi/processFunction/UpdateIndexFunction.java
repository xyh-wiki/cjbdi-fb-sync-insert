package com.cjbdi.processFunction;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cjbdi.config.YamlManager;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;

public class UpdateIndexFunction extends ProcessFunction<String, Void> {
    private transient Connection conn;
    private static String dbId;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Class.forName("org.postgresql.Driver");
        String url = YamlManager.getPostgresIndexUrl();
        String username = YamlManager.getPostgresIndexUsername();
        String password = YamlManager.getPostgresIndexPassword();
        dbId = YamlManager.getPostgresSourceDbId();

        conn = DriverManager.getConnection(url, username, password);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<Void> out) throws Exception {

        String schemaName = "index_" + dbId;

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
                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
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
                try (PreparedStatement pstmt = conn.prepareStatement(deleteSql)) {
                    pstmt.setString(1, jsonObject.getString("c_stm"));
                    pstmt.executeUpdate();
                }
                break;
            case 2: // 修改
                String updateSql = String.format(
                    "UPDATE %s SET d_xgsj = ?, c_baah = ?, n_jbfy = ?, update_time = NOW(), data_state = 2 WHERE c_stm = ?",
                    indexTableName);
                try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {

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

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }
}
