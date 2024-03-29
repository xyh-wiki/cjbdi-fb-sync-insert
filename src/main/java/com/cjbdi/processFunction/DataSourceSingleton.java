package com.cjbdi.processFunction;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.java.utils.ParameterTool;

public class DataSourceSingleton {
    private static volatile HikariDataSource dataSource = null;

    public static HikariDataSource getDataSource(ParameterTool parameterTool) {
        if (dataSource == null) {
            synchronized (DataSourceSingleton.class) {
                if (dataSource == null) {
                    HikariConfig config = new HikariConfig();
                    config.setJdbcUrl(parameterTool.get("postgres.url"));
                    config.setUsername(parameterTool.get("postgres.username"));
                    config.setPassword(parameterTool.get("postgres.password"));

                    config.setMaximumPoolSize(1);
                    // 其他配置...
                    dataSource = new HikariDataSource(config);
                }
            }
        }
        return dataSource;
    }
}
