import com.alibaba.fastjson.JSONObject;

/**
 * @ClassName: JSONDemo
 * @Time: 2024/4/10 15:47
 * @Author: XYH
 * @Description: TODO
 */
public class JSONDemo {
    public static void main(String[] args) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("a", 1);
        jsonObject.put("a", 2);

        System.out.println(jsonObject.getString("ff"));
    }
}
