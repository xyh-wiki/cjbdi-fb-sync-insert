package com.cjbdi.processFunction;

import com.cjbdi.bean.SourceTableData;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PostgresSource extends RichFlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {

    }
}
