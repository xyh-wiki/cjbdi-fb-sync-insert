package com.cjbdi.utils;

/**
 * @Author: XYH
 * @Date: 2023/4/17 12:30
 * @Description: TODO
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;


/**
 * @Author: XYH
 * @Date: 2022/9/26 3:09 PM
 * @Description: 自定义hdfs桶id相关文件信息
 */
public class WriteHdfsBucketAssigner implements BucketAssigner<String, String> {

    @Override
    public String getBucketId(String element, Context context) {

        JSONObject jsonObject = JSON.parseObject(element);

        String table = jsonObject.getString("tableName");
        String database = jsonObject.getString("database");
        String dt = jsonObject.getString("dt");
        return database + ".db" + "/" + table + "/" + "dt=" + dt;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return new WriteHdfsStringSerializer();
    }
}
