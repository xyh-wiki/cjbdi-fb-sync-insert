package com.cjbdi.config;

import com.cjbdi.utils.YamlUtils;
import java.util.Map;


/**
 * @Author: XYH
 * @Date: 2024/5/14 23:20
 * @Description: TODO
 */
public class YamlManager {

    private static Map<String, Object> config;

    public static void setConfiguration(Map<String, Object> configuration) {
        YamlManager.config = configuration;
    }

    public static class Keys {
        public static final String CHECKPOINT_DIR = "checkpoint.dir";
        public static final String KAFKA_SERVER = "kafka.server";
        public static final String KAFKA_TOPIC = "kafka.topic";
        public static final String KAFKA_GROUP_ID = "kafka.input.groupId";
        public static final String WAREHOUSE_PATH = "warehouse.path";
        public static final String WAREHOUSE_USERNAME = "warehouse.username";
        public static final String POSTGRES_SOURCE_URL = "postgres.source.url";
        public static final String POSTGRES_SOURCE_USERNAME = "postgres.source.username";
        public static final String POSTGRES_SOURCE_PASSWORD = "postgres.source.password";
        public static final String POSTGRES_SOURCE_DBID = "postgres.source.dbid";
        public static final String POSTGRES_SOURCE_CONNECTIONS = "postgres.source.connections";
        public static final String POSTGRES_INDEX_URL = "postgres.index.url";
        public static final String POSTGRES_INDEX_USERNAME = "postgres.index.username";
        public static final String POSTGRES_INDEX_PASSWORD = "postgres.index.password";
        public static final String JOB_KAFKASOURCE_PARALLELISM = "job.parallelism.kafkaSource";
        public static final String JOB_HDFSSINK_PARALLELISM = "job.parallelism.hdfsSink";
        public static final String JOB_INDEXSINK_PARALLELISM = "job.parallelism.indexSink";
        public static final String WAREHOUSE_DATABASE = "warehouse.database";
    }

    public static String getWarehouseDatabase() {
        return YamlUtils.getString(config, Keys.WAREHOUSE_DATABASE);
    }
    public static String getWarehouseDatabase(String defaultValue) {
        return YamlUtils.getString(config, Keys.WAREHOUSE_DATABASE, defaultValue);
    }

    public static int getJobIndexSinkParallelism() {
        return YamlUtils.getInt(config, Keys.JOB_INDEXSINK_PARALLELISM);
    }
    public static int getJobIndexSinkParallelism(int defaultValue) {
        return YamlUtils.getInt(config, Keys.JOB_INDEXSINK_PARALLELISM, defaultValue);
    }

    public static int getJobHdfsSinkParallelism() {
        return YamlUtils.getInt(config, Keys.JOB_HDFSSINK_PARALLELISM);
    }
    public static int getJobHdfsSinkParallelism(int defaultValue) {
        return YamlUtils.getInt(config, Keys.JOB_HDFSSINK_PARALLELISM, defaultValue);
    }

    public static int getJobSourceParallelism(){
        return YamlUtils.getInt(config, Keys.JOB_KAFKASOURCE_PARALLELISM);
    }
    public static int getJobSourceParallelism(int defaultValue){
        return YamlUtils.getInt(config, Keys.JOB_KAFKASOURCE_PARALLELISM, defaultValue);
    }

    public static int getPostgresSourceConnections() {
        return YamlUtils.getInt(config, Keys.POSTGRES_SOURCE_CONNECTIONS);
    }
    public static int getPostgresSourceConnections(int defaultValue) {
        return YamlUtils.getInt(config, Keys.POSTGRES_SOURCE_CONNECTIONS, defaultValue);
    }

    public static String getCheckpointDir() {
        return YamlUtils.getString(config, Keys.CHECKPOINT_DIR);
    }
    public static String getCheckpointDir(String defaultValue) {
        return YamlUtils.getString(config, Keys.CHECKPOINT_DIR, defaultValue);
    }

    public static String getKafkaServer() {
        return YamlUtils.getString(config, Keys.KAFKA_SERVER);
    }
    public static String getKafkaServer(String defaultValue) {
        return YamlUtils.getString(config, Keys.KAFKA_SERVER, defaultValue);
    }

    public static String getKafkaTopic() {
        return YamlUtils.getString(config, Keys.KAFKA_TOPIC);
    }
    public static String getKafkaTopic(String defaultValue) {
        return YamlUtils.getString(config, Keys.KAFKA_TOPIC, defaultValue);
    }

    public static String getKafkaGroupId() {
        return YamlUtils.getString(config, Keys.KAFKA_GROUP_ID);
    }
    public static String getKafkaGroupId(String defaultValue) {
        return YamlUtils.getString(config, Keys.KAFKA_GROUP_ID, defaultValue);
    }

    public static String getWarehousePath() {
        return YamlUtils.getString(config, Keys.WAREHOUSE_PATH);
    }
    public static String getWarehousePath(String defaultValue) {
        return YamlUtils.getString(config, Keys.WAREHOUSE_PATH, defaultValue);
    }

    public static String getWarehouseUsername() {
        return YamlUtils.getString(config, Keys.WAREHOUSE_USERNAME);
    }
    public static String getWarehouseUsername(String defaultValue) {
        return YamlUtils.getString(config, Keys.WAREHOUSE_USERNAME, defaultValue);
    }

    public static String getPostgresSourceUrl() {
        return YamlUtils.getString(config, Keys.POSTGRES_SOURCE_URL);
    }
    public static String getPostgresSourceUrl(String defaultValue) {
        return YamlUtils.getString(config, Keys.POSTGRES_SOURCE_URL, defaultValue);
    }

    public static String getPostgresSourceUsername() {
        return YamlUtils.getString(config, Keys.POSTGRES_SOURCE_USERNAME);
    }
    public static String getPostgresSourceUsername(String defaultValue) {
        return YamlUtils.getString(config, Keys.POSTGRES_SOURCE_USERNAME, defaultValue);
    }

    public static String getPostgresSourcePassword() {
        return YamlUtils.getString(config, Keys.POSTGRES_SOURCE_PASSWORD);
    }
    public static String getPostgresSourcePassword(String defaultValue) {
        return YamlUtils.getString(config, Keys.POSTGRES_SOURCE_PASSWORD, defaultValue);
    }

    public static String getPostgresSourceDbId() {
        return YamlUtils.getString(config, Keys.POSTGRES_SOURCE_DBID);
    }
    public static String getPostgresSourceDbId(String defaultValue) {
        return YamlUtils.getString(config, Keys.POSTGRES_SOURCE_DBID, defaultValue);
    }

    public static String getPostgresIndexUrl() {
        return YamlUtils.getString(config, Keys.POSTGRES_INDEX_URL);
    }
    public static String getPostgresIndexUrl(String defaultValue) {
        return YamlUtils.getString(config, Keys.POSTGRES_INDEX_URL, defaultValue);
    }

    public static String getPostgresIndexUsername() {
        return YamlUtils.getString(config, Keys.POSTGRES_INDEX_USERNAME);
    }
    public static String getPostgresIndexUsername(String defaultValue) {
        return YamlUtils.getString(config, Keys.POSTGRES_INDEX_USERNAME, defaultValue);
    }

    public static String getPostgresIndexPassword() {
        return YamlUtils.getString(config, Keys.POSTGRES_INDEX_PASSWORD);
    }
    public static String getPostgresIndexPassword(String defaultValue) {
        return YamlUtils.getString(config, Keys.POSTGRES_INDEX_PASSWORD, defaultValue);
    }
}
