package com.cjbdi.utils;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


/**
 * @Author: XYH
 * @Date: 2022/9/26 3:09 PM
 * @Description: hdfs 写入序列化
 */
public class WriteHdfsStringSerializer implements SimpleVersionedSerializer<String> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(String s) throws IOException {
        return s.getBytes(StandardCharsets.UTF_8);

    }

    @Override
    public String deserialize(int i, byte[] bytes) throws IOException {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}