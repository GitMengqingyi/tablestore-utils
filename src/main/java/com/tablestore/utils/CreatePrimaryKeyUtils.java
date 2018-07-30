package com.tablestore.utils;
/**
 * This file created by mengqingyi on 2017-11-13.
 */

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * 类文件注释(Class file)
 *
 * @author mengqingyi
 * @classDescription 表格存储一些通用的工具类，包括创建主键等
 * @create 2017-11-13 14:51
 **/
public class CreatePrimaryKeyUtils {
    private static final String HEX_CHARS = "0123456789abcdef";
    private static final int PRIMARY_KEY_NUM = 3;
    private static final Logger logger = LoggerFactory.getLogger("log.tableStore.CreatePrimaryKeyUtils");

    /**
     * describe: TODO 构造生成表格存储主键, 分区键为MD5之后的分区键。
     **/
    public static PrimaryKey createPrimaryKey(String partitionKey, String parameterKey) {
        // 首先对入参进行判断 若为空直接返回
        if (StringUtils.isBlank(partitionKey) || StringUtils.isBlank(parameterKey)) {
            logger.error("分区键{}和主键{}中存在空值,主键构造失败", partitionKey, parameterKey);
            return null;
        }
        //对分区键MD5
        partitionKey = md5Hex(partitionKey);
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("partitionKey", PrimaryKeyValue.fromString(partitionKey));
        primaryKeyBuilder.addPrimaryKeyColumn("parameterKey", PrimaryKeyValue.fromString(parameterKey));
        return primaryKeyBuilder.build();
    }

    /**
     * 使用可变参数类型的 主键keys来构建表格存储主键。如果分区键和主键keys中存在空值,不再构造主键 需要注意的是,可变参数最好应该控制在3个以内！
     */
    public static PrimaryKey createPrimaryKey(String partitionKey, String... parameterKeys) {
        if (parameterKeys.length > PRIMARY_KEY_NUM) {
            logger.error("表格存储主键最多只能有4个/列,超出主键列最大限制,主键构造失败,parameterKeys{}", Arrays.toString(parameterKeys));
            return null;
        }
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        // 首先对入参进行判断 若为空直接返回
        if (!StringUtils.isBlank(partitionKey)) {
            //对分区键MD5
            partitionKey = md5Hex(partitionKey);
            primaryKeyBuilder.addPrimaryKeyColumn("partitionKey", PrimaryKeyValue.fromString(partitionKey));
        } else {
            logger.error("分区键为空，主键构造失败");
            return null;
        }
        int i = 1;
        for (String parameterKey : parameterKeys) {
            if (!StringUtils.isBlank(parameterKey)) {
                primaryKeyBuilder.addPrimaryKeyColumn("parameterKey_" + i, PrimaryKeyValue.fromString(parameterKey));
                i++;
            } else {
                logger.error("主键列中存在空字符串，主键构造失败");
                return null;
            }
        }
        return primaryKeyBuilder.build();
    }

    /**
     * describe: TODO 构造生成表格存储主键, 分区键为MD5之后的分区键。
     **/
    public static PrimaryKey createLongPrimaryKey(Long partitionKey, Long parameterKey) {
        // 首先对入参进行判断 若为空直接返回 不再查询
        if (partitionKey == null || parameterKey == null) {
            logger.error("分区键或主键列中存在空值,主键构造失败");
            return null;
        }
        //对分区键MD5
        String partitionKeyStr = md5Hex((partitionKey + ""));
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("partitionKey", PrimaryKeyValue.fromString(partitionKeyStr));
        primaryKeyBuilder.addPrimaryKeyColumn("parameterKey", PrimaryKeyValue.fromLong(parameterKey));
        return primaryKeyBuilder.build();
    }

    /**
     * MD5系列算法
     */

    private static String toHexString(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte aB : b) {
            sb.append(HEX_CHARS.charAt(aB >>> 4 & 0x0F));
            sb.append(HEX_CHARS.charAt(aB & 0x0F));
        }
        return sb.toString();
    }

    private static MessageDigest getDigest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] md5(byte[] data) {
        return getDigest().digest(data);
    }

    private static byte[] md5(String data) {
        return md5(data.getBytes());
    }

    /**
     * 对String字符串进行MD5哈希
     */
    private static String md5Hex(String data) {
        return toHexString(md5(data));
    }

}
