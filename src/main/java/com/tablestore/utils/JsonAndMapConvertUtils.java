package com.tablestore.utils;
/**
 * This file created by mengqingyi on 2017-11-17.
 */

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import com.alibaba.fastjson.JSON;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 类文件注释(Class file)
 *
 * @author mengqingyi
 * @classDescription Json和Map互转类
 * @create 2017-11-17 17:01
 **/
public class JsonAndMapConvertUtils {
    /**
     * hashMap默认大小
     */
    private static final int INITIAL_CAPACITY = 16;
    /**
     * 日志
     */
    private static final Logger logger = LoggerFactory.getLogger("log.tableStore.JsonAndMapConvertUtils");

    /**
     * 将json转换为map,第二个参数为是否为压缩之后的json
     */
    public static Map jsonToMap(String jsonStr, Boolean isCompress) {
        if (StringUtils.isBlank(jsonStr)) {
            return new HashMap<>(INITIAL_CAPACITY);
        }
        if (isCompress) {
            try {
                jsonStr = GzipUtil.unCompress(jsonStr);
            } catch (IOException e) {
                logger.error("解压缩失败e:{}", e.getMessage());
                e.printStackTrace();
                return new HashMap<>(INITIAL_CAPACITY);
            }
        }
        if (!isJson(jsonStr)) {
            return new HashMap<>(INITIAL_CAPACITY);
        }
        return JSON.parseObject(jsonStr, Map.class);
    }

    /**
     * 判断是否为json字符串
     */
    private static Boolean isJson(String jsonStr) {
        try {
            new JsonParser().parse(jsonStr);
            return Boolean.TRUE;
        } catch (JsonSyntaxException e) {
            logger.error("非json字符串,转换失败e:{},将返回空对象", e.getMessage());
            e.printStackTrace();
            return Boolean.FALSE;
        }
    }

    /**
     * map转为json字符串
     */
    public static String mapToJson(Map map) {
        if (map.isEmpty()) {
            return "{}";
        }
        return JSON.toJSONString(map);
    }
}
