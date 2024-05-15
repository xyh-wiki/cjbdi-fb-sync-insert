//
//import com.cjbdi.utils.YamlUtils;
//import org.junit.jupiter.api.Test;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import static org.junit.jupiter.api.Assertions.*;
//
///**
// * @Author: XYH
// * @Date: 2024/5/14 23:20
// * @Description: TODO
// */
//public class YamlUtilsTest {
//
//    @Test
//    public void testLoadYamlFromEnv() {
//        // 为了测试该方法，你需要设置一个环境变量，指向一个存在的YAML文件路径
//        String envVariable = "YAML_FILE_PATH"; // 请替换为你实际的环境变量名称
//        Map<String, Object> yamlMap = YamlUtils.loadYamlFromEnv(envVariable);
//        assertNotNull(yamlMap);
//        // 根据你的YAML文件内容，编写对应的断言以验证加载是否成功
//        // 例如：
//        assertTrue(yamlMap.containsKey("checkpoint"));
//        assertTrue(yamlMap.get("checkpoint") instanceof Map);
//    }
//
//    @Test
//    public void testLoadYaml() {
//        // 请提供一个存在的YAML文件路径用于测试
//        String yamlFilePath = "E:\\02-code\\cjbdi-fb-sync-insert\\src\\main\\resources\\config.yaml"; // 请替换为你实际的YAML文件路径
//        Map<String, Object> yamlMap = YamlUtils.loadYaml(yamlFilePath);
//        assertNotNull(yamlMap);
//        // 根据YAML文件内容，编写对应的断言以验证加载是否成功
//        assertTrue(yamlMap.containsKey("checkpoint"));
//        assertTrue(yamlMap.get("checkpoint") instanceof Map);
//    }
//
//    @Test
//    public void testGetString() {
//        // 构造一个测试用的YAML映射
//        Map<String, Object> yamlMap = new HashMap<>();
//        yamlMap.put("key1", "value1");
//        yamlMap.put("key2", 123);
//
//        // 测试获取字符串值
//        assertEquals("value1", YamlUtils.getString(yamlMap, "key1"));
//        assertEquals("default", YamlUtils.getString(yamlMap, "nonexistent", "default"));
//    }
//
////    @Test
////    public void testGetInt() {
////        // 构造一个测试用的YAML映射
////        Map<String, Object> yamlMap = new HashMap<>();
////        yamlMap.put("key1", "123");
////        yamlMap.put("key2", 456);
////
////        // 测试获取整数值
////        assertEquals(123, YamlUtils.getInt(yamlMap, "key1"));
////        assertEquals(456, YamlUtils.getInt(yamlMap, "key2"));
////        assertEquals(0, YamlUtils.getInt(yamlMap, "nonexistent"));
////        assertEquals(789, YamlUtils.getInt(yamlMap, "nonexistent", 789));
////    }
//
////    @Test
////    public void testGetBoolean() {
////        // 构造一个测试用的YAML映射
////        Map<String, Object> yamlMap = new HashMap<>();
////        yamlMap.put("key1", true);
////        yamlMap.put("key2", false);
////
////        // 测试获取布尔值
////        assertTrue(YamlUtils.getBoolean(yamlMap, "key1"));
////        assertFalse(YamlUtils.getBoolean(yamlMap, "key2"));
////        assertFalse(YamlUtils.getBoolean(yamlMap, "nonexistent"));
////        assertTrue(YamlUtils.getBoolean(yamlMap, "nonexistent", true));
////    }
//}
