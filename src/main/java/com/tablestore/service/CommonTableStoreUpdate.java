package com.tablestore.service;
/**
 * This file created by mengqingyi on 2017-11-14.
 */

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.UpdateRowRequest;
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

import java.util.Map;

/**
 * 类文件注释(Class file)
 *
 * @author mengqingyi
 * @classDescription 通用表格存储更新方法
 * @create 2017-11-14 14:32
 **/
@Repository
public class CommonTableStoreUpdate {
    @Autowired
    @Qualifier("createClient")
    private SyncClient client;
    private static final Logger logger = LoggerFactory
            .getLogger("log.tableStore.CommonTableStoreSave");

    /**
     * 通过指定 两个long类型的 主键，进行更新操作
     * 代码中支持 不存在则新增 存在则更新。客户端不支持不存在则新增
     * 更新操作对于唯一版本若字段已存在，则覆盖该字段。不存在则会新增该字段
     */
    public Boolean update(Long partitionKey, Long parameterKey,
            CommonTableNameEnum commonTableNameEnum, Map<String, Object> hashMap) {
        String tableName = commonTableNameEnum.getName();
        PrimaryKey primaryKey = CreatePrimaryKeyUtils.createLongPrimaryKey(partitionKey,
                parameterKey);
        if (primaryKey == null || StringUtils.isBlank(tableName) || hashMap.isEmpty()) {
            logger.error("主键primaryKey{}或者表名tableName{}中,hashMap{}存在空值", primaryKey, tableName,
                    hashMap.isEmpty());
            return Boolean.FALSE;
        }
        long startTime = System.currentTimeMillis();
        RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
        // 对传入的map填装到rowPutChange中,对列名和值进行检查,对值进行类型匹配转换
        hashMap.forEach((k, v) -> rowUpdateChange.put(ColumnNameStandardUtils.standard(k),
                ObjectToStringUtils.objectToColumnValue(v)));
        try {
            client.updateRow(new UpdateRowRequest(rowUpdateChange));
            long endTime = System.currentTimeMillis();
            logger.info("表格存储中{}表更新成功主键{},耗时(ms){}", tableName, primaryKey.jsonize(),
                    endTime - startTime);
            return Boolean.TRUE;
        } catch (TableStoreException e) {
            logger.info("表格存储中{}表更新失败主键{},失败原因{}", tableName, primaryKey.jsonize(), e.getMessage());
            e.printStackTrace();
            return Boolean.FALSE;
        } catch (ClientException e1) {
            logger.info("表格存储中{}表更新失败主键{},失败原因{}", tableName, primaryKey.jsonize(), e1.getMessage());
            e1.printStackTrace();
            return Boolean.FALSE;
        } catch (Exception e2) {
            logger.info("表格存储中{}表更新失败主键{},失败原因{}", tableName, primaryKey.jsonize(), e2.getMessage());
            e2.printStackTrace();
            return Boolean.FALSE;
        }
    }

    /**
     * 【通用】指定 parimaryKey进行 更新操作。
     */
    public Boolean update(PrimaryKey primaryKey, CommonTableNameEnum commonTableNameEnum,
            Map<String, Object> hashMap) {
        String tableName = commonTableNameEnum.getName();
        if (primaryKey == null || StringUtils.isBlank(tableName) || hashMap.isEmpty()) {
            logger.error("主键primaryKey{}或者表名tableName{}中,hashMap{}存在空值", primaryKey, tableName,
                    hashMap.isEmpty());
            return Boolean.FALSE;
        }
        long startTime = System.currentTimeMillis();
        RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
        // 对传入的map填装到rowPutChange中,对列名和值进行检查,对值进行类型匹配转换
        hashMap.forEach((k, v) -> rowUpdateChange.put(ColumnNameStandardUtils.standard(k),
                ObjectToStringUtils.objectToColumnValue(v)));
        try {
            client.updateRow(new UpdateRowRequest(rowUpdateChange));
            long endTime = System.currentTimeMillis();
            logger.info("表格存储中{}表更新成功主键{},耗时(ms){}", tableName, primaryKey.jsonize(),
                    endTime - startTime);
            return Boolean.TRUE;
        } catch (TableStoreException e) {
            logger.info("表格存储中{}表更新失败主键{},失败原因{}", tableName, primaryKey.jsonize(), e.getMessage());
            e.printStackTrace();
            return Boolean.FALSE;
        } catch (ClientException e1) {
            logger.info("表格存储中{}表更新失败主键{},失败原因{}", tableName, primaryKey.jsonize(), e1.getMessage());
            e1.printStackTrace();
            return Boolean.FALSE;
        } catch (Exception e2) {
            logger.info("表格存储中{}表更新失败主键{},失败原因{}", tableName, primaryKey.jsonize(), e2.getMessage());
            e2.printStackTrace();
            return Boolean.FALSE;
        }
    }
}
