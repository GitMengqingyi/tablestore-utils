package com.tablestore.service;
/**
 * This file created by mengqingyi on 2017-11-13.
 */

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.tablestore.constant.CommonTableNameEnum;
import com.tablestore.utils.ColumnNameStandardUtils;
import com.tablestore.utils.CreatePrimaryKeyUtils;
import com.tablestore.utils.ObjectToStringUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.Map;

/**
 * 类文件注释(Class file)
 *
 * @author mengqingyi
 * @classDescription 表格存储保存信息一体化通用方法
 * @create 2017-11-13 14:49
 **/
@Repository
public class CommonTableStoreSave {
    @Autowired
    @Qualifier("createClient")
    private SyncClient client;
    private static final Logger logger = LoggerFactory
            .getLogger("log.tableStore.CommonTableStoreSave");

    /**
     * describe: TODO 通用表格存储保存方法，适用于 一个String分区键一个String主键共同构造主键，数据格式为map<String,Object>
     *
     * method_name: save param: [tableName, partitionKey, parameterKey, hashMap]
     * result_type:java.lang.Boolean
     *
     * creat_user: 孟庆艺 creat_date:2017-11-13 creat_time:16:45
     **/
    public Boolean save(CommonTableNameEnum commonTableNameEnum, Map<String, Object> hashMap,
            String partitionKey, String parameterKey) {
        String tableName = commonTableNameEnum.getName();
        long startTime = System.currentTimeMillis();
        PrimaryKey primaryKey = CreatePrimaryKeyUtils.createPrimaryKey(partitionKey, parameterKey);
        // 如果主键为空,参数有误,直接返回false
        if (primaryKey == null || StringUtils.isBlank(tableName) || hashMap.isEmpty()) {
            logger.error("主键primaryKey{}或者表名tableName{}中,hashMap{}存在空值", primaryKey, tableName,
                    hashMap.isEmpty());
            return Boolean.FALSE;
        }
        tableName = ColumnNameStandardUtils.standard(tableName);
        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);
        // 对传入的map填装到rowPutChange中,对列名和值进行检查,对值进行类型匹配转换
        hashMap.forEach((k, v) -> rowPutChange.addColumn(ColumnNameStandardUtils.standard(k),
                ObjectToStringUtils.objectToColumnValue(v)));
        try {
            client.putRow(new PutRowRequest(rowPutChange));
            long endTime = System.currentTimeMillis();
            logger.info("表格存储中{}表新增分区键{},主键{}成功,耗时(ms){}", tableName, partitionKey, parameterKey,
                    endTime - startTime);
            return Boolean.TRUE;
        } catch (TableStoreException e) {
            logger.info("表格存储中{}表新增分区键{},主键{}失败,失败原因{}", tableName, partitionKey, parameterKey,
                    e.getMessage());
            e.printStackTrace();
            return Boolean.FALSE;
        } catch (ClientException e1) {
            logger.info("表格存储中{}表新增分区键{},主键{}失败,失败原因{}", tableName, partitionKey, parameterKey,
                    e1.getMessage());
            e1.printStackTrace();
            return Boolean.FALSE;
        } catch (Exception e2) {
            logger.info("表格存储中{}表新增分区键{},主键{}失败,失败原因{}", tableName, partitionKey, parameterKey,
                    e2.getMessage());
            e2.printStackTrace();
            return Boolean.FALSE;
        }
    }

    /**
     * describe: TODO 可变参数构造表格存储主键,用于保存表格存储,适用于
     * 一个String分区键多个String类型主键共同构造主键，数据格式为map<String,Object>
     *
     * method_name: save param: [tableName, hashMap, partitionKey, parameterKeys]
     * result_type:java.lang.Boolean
     *
     * creat_user: 孟庆艺 creat_date:2017-11-13 creat_time:16:58
     **/
    public Boolean save(CommonTableNameEnum commonTableNameEnum, Map<String, Object> hashMap,
            String partitionKey, String... parameterKeys) {
        String tableName = commonTableNameEnum.getName();
        long startTime = System.currentTimeMillis();
        PrimaryKey primaryKey = CreatePrimaryKeyUtils.createPrimaryKey(partitionKey, parameterKeys);
        // 如果主键为空,参数有误,直接返回false
        if (primaryKey == null || StringUtils.isBlank(tableName) || hashMap.isEmpty()) {
            logger.error("主键primaryKey{}或者表名tableName{}中,hashMap{}存在空值", primaryKey, tableName,
                    hashMap.isEmpty());
            return Boolean.FALSE;
        }
        tableName = ColumnNameStandardUtils.standard(tableName);
        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);
        // 对传入的map填装到rowPutChange中,对列名和值进行检查,对值进行类型匹配转换
        hashMap.forEach((k, v) -> rowPutChange.addColumn(ColumnNameStandardUtils.standard(k),
                ObjectToStringUtils.objectToColumnValue(v)));
        try {
            client.putRow(new PutRowRequest(rowPutChange));
            long endTime = System.currentTimeMillis();
            logger.info("表格存储中{}表新增分区键{},主键{}成功,耗时(ms){}", tableName, partitionKey,
                    Arrays.toString(parameterKeys), endTime - startTime);
            return Boolean.TRUE;
        } catch (TableStoreException e) {
            logger.info("表格存储中{}表新增分区键{},主键{}失败,失败原因{}", tableName, partitionKey,
                    Arrays.toString(parameterKeys), e.getMessage());
            e.printStackTrace();
            return Boolean.FALSE;
        } catch (ClientException e1) {
            logger.info("表格存储中{}表新增分区键{},主键{}失败,失败原因{}", tableName, partitionKey,
                    Arrays.toString(parameterKeys), e1.getMessage());
            e1.printStackTrace();
            return Boolean.FALSE;
        } catch (Exception e2) {
            logger.info("表格存储中{}表新增分区键{},主键{}失败,失败原因{}", tableName, partitionKey,
                    Arrays.toString(parameterKeys), e2.getMessage());
            e2.printStackTrace();
            return Boolean.FALSE;
        }
    }

    /**
     * describe: TODO long类型的主键用来保存数据,适用于long类型分区键和主键
     *
     * method_name: save param: [tableName, hashMap, partitionKey, parameterKey]
     * result_type:java.lang.Boolean
     *
     * creat_user: 孟庆艺 creat_date:2017-11-13 creat_time:16:59
     **/
    public Boolean save(CommonTableNameEnum commonTableNameEnum, Map<String, Object> hashMap,
            Long partitionKey, Long parameterKey) {
        String tableName = commonTableNameEnum.getName();
        long startTime = System.currentTimeMillis();
        PrimaryKey primaryKey = CreatePrimaryKeyUtils.createLongPrimaryKey(partitionKey,
                parameterKey);
        // 如果主键为空,参数有误,直接返回false
        if (primaryKey == null || StringUtils.isBlank(tableName) || hashMap.isEmpty()) {
            logger.error("主键primaryKey{}或者表名tableName{}中,hashMap{}存在空值", primaryKey, tableName,
                    hashMap.isEmpty());
            return Boolean.FALSE;
        }
        tableName = ColumnNameStandardUtils.standard(tableName);
        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);
        // 对传入的map填装到rowPutChange中,对列名和值进行检查,对值进行类型匹配转换
        hashMap.forEach((k, v) -> rowPutChange.addColumn(ColumnNameStandardUtils.standard(k),
                ObjectToStringUtils.objectToColumnValue(v)));
        try {
            client.putRow(new PutRowRequest(rowPutChange));
            long endTime = System.currentTimeMillis();
            logger.info("表格存储中{}表新增分区键{},主键{}成功,耗时(ms){}", tableName, partitionKey, parameterKey,
                    endTime - startTime);
            return Boolean.TRUE;
        } catch (TableStoreException e) {
            logger.info("表格存储中{}表新增分区键{},主键{}失败,失败原因{}", tableName, partitionKey, parameterKey,
                    e.getMessage());
            e.printStackTrace();
            return Boolean.FALSE;
        } catch (ClientException e1) {
            logger.info("表格存储中{}表新增分区键{},主键{}失败,失败原因{}", tableName, partitionKey, parameterKey,
                    e1.getMessage());
            e1.printStackTrace();
            return Boolean.FALSE;
        } catch (Exception e2) {
            logger.info("表格存储中{}表新增分区键{},主键{}失败,失败原因{}", tableName, partitionKey, parameterKey,
                    e2.getMessage());
            e2.printStackTrace();
            return Boolean.FALSE;
        }
    }

    /**
     * describe: TODO 对传入的分区键 主键等由使用者自行定义,然后进行新增操作
     *
     * method_name: save param: [tableName,hashMap,primaryKey] result_type:Boolean
     *
     * creat_user: 孟庆艺 creat_date:2017-11-14 creat_time:14:44
     **/
    public Boolean save(CommonTableNameEnum commonTableNameEnum, Map<String, Object> hashMap,
            PrimaryKey primaryKey) {
        String tableName = commonTableNameEnum.getName();
        long startTime = System.currentTimeMillis();
        // 如果主键为空,参数有误,直接返回false
        if (primaryKey == null || StringUtils.isBlank(tableName) || hashMap.isEmpty()) {
            logger.error("主键primaryKey{}或者表名tableName{}中,hashMap{}存在空值", primaryKey.jsonize(),
                    tableName, hashMap.isEmpty());
            return Boolean.FALSE;
        }
        tableName = ColumnNameStandardUtils.standard(tableName);
        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);
        // 对传入的map填装到rowPutChange中,对列名和值进行检查,对值进行类型匹配转换
        hashMap.forEach((k, v) -> rowPutChange.addColumn(ColumnNameStandardUtils.standard(k),
                ObjectToStringUtils.objectToColumnValue(v)));
        try {
            client.putRow(new PutRowRequest(rowPutChange));
            long endTime = System.currentTimeMillis();
            logger.info("表格存储中{}表新增主键{}成功,耗时(ms){}", tableName, primaryKey.jsonize(),
                    endTime - startTime);
            return Boolean.TRUE;
        } catch (TableStoreException e) {
            logger.info("表格存储中{}表新增主键{}失败,失败原因{}", tableName, primaryKey.jsonize(), e.getMessage());
            e.printStackTrace();
            return Boolean.FALSE;
        } catch (ClientException e1) {
            logger.info("表格存储中{}表新增主键{}失败,失败原因{}", tableName, primaryKey.jsonize(),
                    e1.getMessage());
            e1.printStackTrace();
            return Boolean.FALSE;
        } catch (Exception e2) {
            logger.info("表格存储中{}表新增主键{}失败,失败原因{}", tableName, primaryKey.jsonize(),
                    e2.getMessage());
            e2.printStackTrace();
            return Boolean.FALSE;
        }
    }
}
