package com.cjbdi.utils;

import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

/**
 * @Author: XYH
 * @Date: 2024/5/14 23:20
 * @Description: Yaml 工具类
 */
@Slf4j
public class YamlUtils {
    private static final Yaml yaml = new Yaml();

    public static Map<String, Object> loadYamlFromEnv(String envVariable) {
        String filePath = System.getenv(envVariable);
        if (filePath == null) {
            throw new IllegalArgumentException("环境变量未找到!!!: " + envVariable);
        }
        return loadYaml(filePath);
    }

    public static Map<String, Object> loadYaml(String filePath) {
        try {
            InputStream inputStream = new FileInputStream(filePath);
            return yaml.load(inputStream);
        } catch (FileNotFoundException e) {
            log.error("配置文件加载失败>>{} ", e.getMessage());
            return null;
        }
    }

    public static String getString(Map<String, Object> yamlMap, String key, String defaultValue) {
        Object value = getValue(yamlMap, key);
        return value != null ? value.toString() : defaultValue;
    }

    public static int getInt(Map<String, Object> yamlMap, String key, int defaultValue) {
        Object value = getValue(yamlMap, key);
        return value instanceof Number ? ((Number) value).intValue() : defaultValue;
    }

    public static boolean getBoolean(Map<String, Object> yamlMap, String key, boolean defaultValue) {
        Object value = getValue(yamlMap, key);
        return value instanceof Boolean ? (Boolean) value : defaultValue;
    }

    public static String getString(Map<String, Object> yamlMap, String key) {
        return (String) getValue(yamlMap, key);
    }

    public static int getInt(Map<String, Object> yamlMap, String key) {
        return (int) getValue(yamlMap, key);
    }

    public static boolean getBoolean(Map<String, Object> yamlMap, String key) {
        return (boolean) getValue(yamlMap, key);
    }


    private static Object getValue(Map<String, Object> yamlMap, String key) {
        String[] keys = key.split("\\.");
        return getValueRecursively(yamlMap, keys, 0);
    }

    private static Object getValueRecursively(Map<String, Object> yamlMap, String[] keys, int index) {
        if (index >= keys.length) {
            return null; // 键值不存在
        }

        Object value = yamlMap.get(keys[index]);
        if (value instanceof Map) {
            // 如果值是 Map 类型，则递归获取下一级的值
            return getValueRecursively((Map<String, Object>) value, keys, index + 1);
        } else {
            // 如果值不是 Map 类型，则说明已经到达最终的值
            return value;
        }
    }
}
