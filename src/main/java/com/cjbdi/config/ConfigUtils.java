package com.cjbdi.config;

import com.cjbdi.utils.YamlUtils;
import java.util.Map;


/**
 * @Author: XYH
 * @Date: 2024/5/14 23:20
 * @Description: TODO
 */
public class ConfigUtils {

    // Constants for configuration keys
    public static class Keys {
        public static final String DATABASE_ID = "database.sourceDb.dbid";
        public static final String HIVE_DATABASE_PATH = "database.hive.dbPath";
        public static final String INDEX_DATABASE_URL = "database.indexDb.url";
        public static final String INDEX_DATABASE_USERNAME = "database.indexDb.username";
        public static final String INDEX_DATABASE_PASSWORD = "database.indexDb.password";
        public static final String SOURCE_DATABASE_URL = "database.sourceDb.url";
        public static final String SOURCE_DATABASE_USERNAME = "database.sourceDb.username";
        public static final String SOURCE_DATABASE_PASSWORD = "database.sourceDb.password";
        public static final String SOURCE_DATABASE_SCHEMA_REGEX = "database.sourceDb.schema.regex";
        public static final String SOURCE_DATABASE_TABLE_REGEX = "database.sourceDb.table.regex";
        public static final String JOB_MODE = "job.mode";
        public static final String SPARK_MASTER = "spark.master";
        public static final String KAFKA_SERVER = "kafka.server";
        public static final String KAFKA_SINK_TOPIC = "kafka.sinkTopic";
        public static final String HADOOP_USERNAME = "warehouse.username";
    }

    private static Map<String, Object> config;

    public static void setConfiguration(Map<String, Object> configuration) {
        ConfigUtils.config = configuration;
    }

    public static String getSourceDatabaseId(String defaultValue) {
        return YamlUtils.getString(config, Keys.DATABASE_ID, defaultValue);
    }

    public static String getHiveDatabasePath(String defaultValue) {
        return YamlUtils.getString(config, Keys.HIVE_DATABASE_PATH, defaultValue);
    }

    public static String getIndexDatabaseUrl(String defaultValue) {
        return YamlUtils.getString(config, Keys.INDEX_DATABASE_URL, defaultValue);
    }

    public static String getIndexDatabaseUsername(String defaultValue) {
        return YamlUtils.getString(config, Keys.INDEX_DATABASE_USERNAME, defaultValue);
    }

    public static String getIndexDatabasePassword(String defaultValue) {
        return YamlUtils.getString(config, Keys.INDEX_DATABASE_PASSWORD, defaultValue);
    }

    public static String getSourceDatabaseUrl(String defaultValue) {
        return YamlUtils.getString(config, Keys.SOURCE_DATABASE_URL, defaultValue);
    }

    public static String getSourceDatabaseUsername(String defaultValue) {
        return YamlUtils.getString(config, Keys.SOURCE_DATABASE_USERNAME, defaultValue);
    }

    public static String getSourceDatabasePassword(String defaultValue) {
        return YamlUtils.getString(config, Keys.SOURCE_DATABASE_PASSWORD, defaultValue);
    }

    public static String getSourceDatabaseSchemaRegex(String defaultValue) {
        return YamlUtils.getString(config, Keys.SOURCE_DATABASE_SCHEMA_REGEX, defaultValue);
    }

    public static String getSourceDatabaseTableRegex(String defaultValue) {
        return YamlUtils.getString(config, Keys.SOURCE_DATABASE_TABLE_REGEX, defaultValue);
    }

    public static String getJobMode(String defaultValue) {
        return YamlUtils.getString(config, Keys.JOB_MODE, defaultValue);
    }

    public static String getSparkMaster(String defaultValue) {
        return YamlUtils.getString(config, Keys.SPARK_MASTER, defaultValue);
    }

    public static String getKafkaServer(String defaultValue) {
        return YamlUtils.getString(config, Keys.KAFKA_SERVER, defaultValue);
    }

    public static String getKafkaSinkTopic(String defaultValue) {
        return YamlUtils.getString(config, Keys.KAFKA_SINK_TOPIC, defaultValue);
    }

    public static String getHadoopUsername(String defaultValue) {
        return YamlUtils.getString(config, Keys.HADOOP_USERNAME, defaultValue);
    }


    public static String getSourceDatabaseId() {
        return YamlUtils.getString(config, Keys.DATABASE_ID);
    }

    public static String getHiveDatabasePath() {
        return YamlUtils.getString(config, Keys.HIVE_DATABASE_PATH);
    }

    public static String getIndexDatabaseUrl() {
        return YamlUtils.getString(config, Keys.INDEX_DATABASE_URL);
    }

    public static String getIndexDatabaseUsername() {
        return YamlUtils.getString(config, Keys.INDEX_DATABASE_USERNAME);
    }

    public static String getIndexDatabasePassword() {
        return YamlUtils.getString(config, Keys.INDEX_DATABASE_PASSWORD);
    }

    public static String getSourceDatabaseUrl() {
        return YamlUtils.getString(config, Keys.SOURCE_DATABASE_URL);
    }

    public static String getSourceDatabaseUsername() {
        return YamlUtils.getString(config, Keys.SOURCE_DATABASE_USERNAME);
    }

    public static String getSourceDatabasePassword() {
        return YamlUtils.getString(config, Keys.SOURCE_DATABASE_PASSWORD);
    }

    public static String getSourceDatabaseSchemaRegex() {
        return YamlUtils.getString(config, Keys.SOURCE_DATABASE_SCHEMA_REGEX);
    }

    public static String getSourceDatabaseTableRegex() {
        return YamlUtils.getString(config, Keys.SOURCE_DATABASE_TABLE_REGEX);
    }

    public static String getJobMode() {
        return YamlUtils.getString(config, Keys.JOB_MODE);
    }

    public static String getSparkMaster() {
        return YamlUtils.getString(config, Keys.SPARK_MASTER);
    }

    public static String getKafkaServer() {
        return YamlUtils.getString(config, Keys.KAFKA_SERVER);
    }

    public static String getKafkaSinkTopic() {
        return YamlUtils.getString(config, Keys.KAFKA_SINK_TOPIC);
    }

    public static String getHadoopUsername() {
        return YamlUtils.getString(config, Keys.HADOOP_USERNAME);
    }
}
