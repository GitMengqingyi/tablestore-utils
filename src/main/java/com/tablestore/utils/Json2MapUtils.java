package com.tablestore.utils;
/**
 * This file created by mengqingyi on 2017-12-15.
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 类文件注释(Class file)
 *
 * @author mengqingyi
 * @classDescription 复杂json转map 但是需要注意 json中的键对应到map中不可重复
 * @create 2017-12-15 17:03
 **/
public class Json2MapUtils {
    /**
     * json 转为 map
     *
     * @param json chuanru 标准的json串
     */
    public static Map json2Map(String json) {
        Map map = new HashMap();
        JSONObject jsonObject = JSONObject.parseObject(json);
        parseJson2Map(jsonObject, map);
        return map;
    }

    /**
     * 将 jsonObject 分析填装到 map
     *
     * @param jsonObject 传入的jsonObject
     * @param map        返回的map
     * @return 返回map
     */
    private static Map<String, Object> parseJson2Map(JSONObject jsonObject, Map map) {
        //JSONObject 第 N 层
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            //转换json第N+1层
            String key = entry.getKey();
            Object value = entry.getValue();
            Object o = JSON.toJSON(value);
            if (o instanceof JSONObject) {
                //内层还是map结构类型
                parseJson2Map((JSONObject) o, map);
            } else if (o instanceof JSONArray) {
                //内层是 list结构类型
                parseArray2Map((JSONArray) o, map);
            } else {
                //普通类型
                map.put(reName(key, map), value);
            }
        }
        return map;
    }

    /**
     * 将 array转为 map 进行保存
     *
     * @param jsonArray 传入的array
     * @param map       传入map
     * @return 返回map
     */
    private static Map parseArray2Map(JSONArray jsonArray, Map map) {
        int index = 0;
        for (Object o : jsonArray) {
            o = JSON.toJSON(o);
            if (o instanceof JSONObject) {
                //内层还是map结构类型
                parseJson2Map((JSONObject) o, map);
            } else if (o instanceof JSONArray) {
                //内层是 list结构类型
                parseArray2Map((JSONArray) o, map);
            } else {
                //普通类型
                map.put(reName(index + "", map), o);
            }
            index++;
        }
        return map;
    }

    /**
     * 检查 map中是否已有 该 key值，如有则重命名。避免数据覆盖
     *
     * @param key map的键名
     * @param map 传入的map
     * @return key+_+当前时间戳
     */
    private static String reName(String key, Map map) {
        String name = key;
        long timeStamp = Instant.now().getEpochSecond();
        for (Object o : map.keySet()) {
            if (key.equals(o.toString())) {
                name = key + "_" + timeStamp;
            }
        }
        return name;
    }

    /* 注释原因 ：启用新 转换实现，旧暂时保留
    public static Map json2Map(String json) {
        LinkedHashMap<String, Object> map = Maps.newLinkedHashMap();
        JSONObject jsonObject = JSONObject.parseObject(json);
        return json2Map(jsonObject, map);
    }

    private static Map json2Map(JSONObject jsonObject, Map map) {
        for (Object o : jsonObject.entrySet()) {
            String entry = String.valueOf(o);
            String key = entry.substring(0, entry.indexOf("="));
            String value = entry.substring(entry.indexOf("=") + 1, entry.length());
            if (jsonObject.get(key).getClass().equals(JSONObject.class)) {
                HashMap _map = new HashMap();
                map.put(key, _map);
                json2Map(jsonObject.getJSONObject(key), _map);
            } else if (jsonObject.get(key).getClass().equals(JSONArray.class)) {
                ArrayList list = new ArrayList();
                map.put(key, list);
                json2Map(jsonObject.getJSONArray(key), list);
            } else {
                map.put(key, jsonObject.get(key));
            }
        }
        return map;
    }

    private static void json2Map(JSONArray jsonArray, List list) {
        IntStream.range(0, jsonArray.size()).forEach(i -> {
            if (jsonArray.get(i).getClass().equals(JSONArray.class)) {
                ArrayList _list = new ArrayList();
                list.add(_list);
                json2Map(jsonArray.getJSONArray(i), _list);
            } else if (jsonArray.get(i).getClass().equals(JSONObject.class)) {
                HashMap _map = new HashMap();
                list.add(_map);
                json2Map(jsonArray.getJSONObject(i), _map);
            } else {
                list.add(jsonArray.get(i));
            }
        });
    }
*/
}
