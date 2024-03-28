package com.cjbdi.dao;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsyncDatabaseRequest extends RichAsyncFunction<String, String> {

    private transient DataSource dataSource;
    private ExecutorService executorService;

    @Override
    public void open(Configuration parameters) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://192.168.50.37:5432/fb15_00");
        config.setUsername("postgres");
        config.setPassword("121019");
        config.setMaximumPoolSize(3);
        this.dataSource = new HikariDataSource(config);
        this.executorService = Executors.newFixedThreadPool(3); // Adjust the pool size according to your needs
    }

    @Override
    public void asyncInvoke(String value, ResultFuture<String> resultFuture) {
        executorService.submit(() -> {
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + value); // Assuming value is the fully qualified table name
                 ResultSet rs = stmt.executeQuery()) {
                 
                StringJoiner joiner = new StringJoiner(",");
                while (rs.next()) {
                    // Assuming you know the columns or they are dynamic
                    joiner.add(rs.getString(1)); // Example for one column, extend it to handle all columns
                }
                resultFuture.complete(Collections.singleton(joiner.toString()));
            } catch (Exception e) {
                resultFuture.completeExceptionally(e);
            }
        });
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }
    }
}
