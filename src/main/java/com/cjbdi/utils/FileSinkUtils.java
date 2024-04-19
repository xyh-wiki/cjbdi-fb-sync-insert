package com.cjbdi.utils;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 * @Author: XYH
 * @Date: 2023/4/12 17:54
 * @Description: hdfs FileSink工具类
 */
public class FileSinkUtils {
    public static FileSink<String> myFileSink(String path) {
        //自定义滚动策略
        DefaultRollingPolicy<String, String> rollPolicy = DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofMinutes(120))
                .withInactivityInterval(Duration.ofMinutes(120))
                .withMaxPartSize(MemorySize.ofMebiBytes(1024 * 1024 * 128))
                .build();

        return FileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new WriteHdfsBucketAssigner())
                .withRollingPolicy(rollPolicy)
                .build();

    }
}
