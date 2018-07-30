package com.tablestore.utils;/**
 * Created by 孟庆艺 on 2017-08-31.
 */

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.BASE64Encoder;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/**
 * 表格存储生成构造主键方法
 *
 * @author mengqingyi
 * @create 2017-08-31 10:13
 **/
public class PrimaryKeyUtils {
    //日志
    private static final Logger logger = LoggerFactory.getLogger(PrimaryKey.class);

    /**
     * 使用md5算法加密算法 对字符串进行加密
     */
    private String EncodeByMd5(String string) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        //计算方法
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        BASE64Encoder base64Encoder = new BASE64Encoder();
        //加密后的字符串
        String newStr = base64Encoder.encode(messageDigest.digest(string.getBytes("UTF-8")));
        return newStr;
    }

    /**
     * 如有明确的分区键，则可以使用分区键+主键格式构造主键
     */
    public PrimaryKey createPrimaryKey(Integer partitionKey, String userId) {
        logger.debug("进入tableStore(core1)生成构造主键通用方法createPrimaryKey");
        // 首先对入参进行判断 若为空直接返回 不再查询
        if (StringUtils.isBlank(partitionKey + "") || StringUtils.isBlank(userId)) {
            logger.warn("表格存储短信详单主键为空,达不到构造主键要求，返回空值");
            return null;
        }
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("partitionKey", PrimaryKeyValue.fromLong(partitionKey));
        primaryKeyBuilder.addPrimaryKeyColumn("userId", PrimaryKeyValue.fromString(userId));
        logger.info("tableStore(core1)生成构造主键通用方法createPrimaryKey：partitionKey={},userId={}", partitionKey, userId);
        return primaryKeyBuilder.build();
    }

    /**
     * 如无明确的分区键，方案1.由大小为5随机桶(实际就是随机数)作为分区键 以桶的随机数作为分片键(表格存储，hbase中也有类似方案，salted key)
     */
    public PrimaryKey createPrimaryKeyByRandomBucket(String userId) {
        logger.debug("进入tableStore(core1)生成构造主键通用方法createPrimaryKey");
        // 首先对入参进行判断 若为空直接返回 不再查询
        if (StringUtils.isBlank(userId)) {
            logger.warn("表格存储短信详单主键为空,达不到构造主键要求，返回空值");
            return null;
        }
        Integer partitionKey = new Random().nextInt(5);
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("partitionKey", PrimaryKeyValue.fromLong(partitionKey));
        primaryKeyBuilder.addPrimaryKeyColumn("userId", PrimaryKeyValue.fromString(userId));
        logger.info("tableStore(core1)生成构造主键通用方法createPrimaryKey：partitionKey={},userId={}", partitionKey, userId);
        return primaryKeyBuilder.build();
    }

    /**
     * 如无明确的分区键，方案1.指定大小的随机桶(实际就是随机数)作为分区键 以桶的随机数作为分片键(表格存储，hbase中也有类似方案，salted key)
     */
    public PrimaryKey createPrimaryKeyByRandomBucket(String userId, Integer randomBucketSize) {
        logger.debug("进入tableStore(core1)生成构造主键通用方法createPrimaryKey");
        // 首先对入参进行判断 若为空直接返回 不再查询
        if (StringUtils.isBlank(userId)) {
            logger.warn("表格存储短信详单主键为空,达不到构造主键要求，返回空值");
            return null;
        }
        Integer partitionKey = new Random().nextInt(randomBucketSize);
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("partitionKey", PrimaryKeyValue.fromLong(partitionKey));
        primaryKeyBuilder.addPrimaryKeyColumn("userId", PrimaryKeyValue.fromString(userId));
        logger.info("tableStore(core1)生成构造主键通用方法createPrimaryKey：partitionKey={},userId={}", partitionKey, userId);
        return primaryKeyBuilder.build();
    }

    /**
     * 无明确的分区键，取散裂化后的userId的前4位作为分区键
     */
    public PrimaryKey createPrimaryKeyByUserId(String userId) {
        logger.debug("进入tableStore(core1)生成构造主键通用方法createPrimaryKey");
        // 首先对入参进行判断 若为空直接返回 不再查询
        if (StringUtils.isBlank(userId)) {
            logger.warn("表格存储短信详单主键为空,达不到构造主键要求，返回空值");
            return null;
        }
        String partitionKey = null;
        try {
            partitionKey = EncodeByMd5(userId);
            logger.info("userId={},MD5之后的md5UserId={}", userId, partitionKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("partitionKey", PrimaryKeyValue.fromString(partitionKey));
        primaryKeyBuilder.addPrimaryKeyColumn("userId", PrimaryKeyValue.fromString(userId));
        logger.info("tableStore(core1)生成构造主键通用方法createPrimaryKey：partitionKey={},userId={}", partitionKey, userId);
        return primaryKeyBuilder.build();
    }


}
