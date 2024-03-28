package com.cjbdi.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

public class DatabaseSchemaTablesFetcher {

    private static DataSource dataSource;
    private static final Set<String> queriedSchemas = new HashSet<>();

    public static void main(String[] args) {
        initDataSource();
        // 假设我们有一个或多个 schema 名称
        String[] schemas = {"public", "your_schema"};
        for (String schema : schemas) {
            fetchAndPrintTablesForSchema(schema);
        }
    }

    private static void initDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://<your_database_host>:<port>/<database_name>");
        config.setUsername("<username>");
        config.setPassword("<password>");
        // 其他配置...
        config.setMaximumPoolSize(3);

        dataSource = new HikariDataSource(config);
    }

    private static void fetchAndPrintTablesForSchema(String schema) {
        // 检查 schema 是否已查询过
        if (!queriedSchemas.contains(schema)) {
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(
                         "SELECT table_name FROM information_schema.tables WHERE table_schema = ?")) {
                stmt.setString(1, schema);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String tableName = rs.getString("table_name");
                        System.out.println(tableName);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            // 标记 schema 为已查询
            queriedSchemas.add(schema);
        } else {
            System.out.println("Schema '" + schema + "' has been queried before. Skipping.");
        }
    }
}
