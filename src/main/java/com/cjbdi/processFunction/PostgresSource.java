package com.cjbdi.processFunction;

import com.cjbdi.bean.SourceTableData;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PostgresSource extends RichSourceFunction<SourceTableData> {
    private volatile boolean isRunning = true;
    private transient Connection connection;
    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String url = parameterTool.get("postgres.url");
        String username = parameterTool.get("postgres.username");
        String password = parameterTool.get("postgres.password");

        connection = DriverManager.getConnection(url, username, password);
        parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    }

    @Override
    public void run(SourceContext<SourceTableData> ctx) throws Exception {
        String sql = "select c_stm, d_xgsj from db_msys.t_msys";

        PreparedStatement pstmt = connection.prepareStatement(sql);
        ResultSet resultSet = pstmt.executeQuery();
        while (resultSet.next() && isRunning) {
            String id = resultSet.getString("c_stm");
            String updateFlag = resultSet.getString("d_xgsj");
            ctx.collect(new SourceTableData(id, updateFlag));
        }
        resultSet.close();
        pstmt.close();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}
