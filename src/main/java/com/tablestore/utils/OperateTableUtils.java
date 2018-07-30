package com.tablestore.utils;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.CapacityUnit;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.ReservedThroughput;
import com.alicloud.openservices.tablestore.model.ReservedThroughputDetails;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.model.UpdateTableRequest;
import com.alicloud.openservices.tablestore.model.internal.CreateTableRequestEx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by 孟庆艺 on 2017-08-03.
 */
@Service
public class OperateTableUtils {
    @Autowired
    @Qualifier("createClient")
    private SyncClient client;
    private static final Logger logger = LoggerFactory.getLogger("log.tableStore.OperateTableUtils");

    /**
     * 创建表
     *
     * @param createTableName 表名
     * @param timeToLive      数据过期时间 单位:秒 例如：一年 365*24*3600 -1表示永不过期
     * @param maxVersions     保存最大版本数 设置为3即代表每列最多保存 N 个最新的版本
     * @param primaryKey      可变参数类型 主键列名 可变参数最大为4
     */
    public void createTable(String createTableName, Integer timeToLive, Integer maxVersions, String... primaryKey) {
        if (primaryKey.length > 4) {
            logger.info("注意:表格存储主键最多可设置4个,可变参数个数最大为4");
            return;
        }
        TableMeta tableMeta = new TableMeta(createTableName);
        for (String PRIMARY_KEY : primaryKey) {
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY, PrimaryKeyType.STRING));
        }
        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);
        CreateTableRequestEx request = new CreateTableRequestEx(tableMeta, tableOptions);
        //设置读写预留值 容量型示例 只能设置为0 高性能示例可以设置为非零值
        request.setReservedThroughput(new ReservedThroughput(new CapacityUnit(0, 0)));
        client.createTable(request);
    }

    /**
     * 创建表
     *
     * @param timeToLive  数据过期时间 单位:秒 例如：一年 365*24*3600 -1表示永不过期
     * @param maxVersions 保存最大版本数 设置为3即代表每列最多保存 N 个最新的版本
     */
    public void createTable(String createTableName, Integer timeToLive, Integer maxVersions,
            List<PrimaryKeySchema> primaryKey) {
        if (primaryKey.size() > 4) {
            logger.info("注意:表格存储主键最多可设置4个,可变参数个数最大为4");
            return;
        }
        TableMeta tableMeta = new TableMeta(createTableName);
        tableMeta.addPrimaryKeyColumns(primaryKey);
        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);
        CreateTableRequestEx request = new CreateTableRequestEx(tableMeta, tableOptions);
        //设置读写预留值 容量型示例 只能设置为0 高性能示例可以设置为非零值
        request.setReservedThroughput(new ReservedThroughput(new CapacityUnit(0, 0)));
        client.createTable(request);
    }


    /**
     * 构建list
     */
    private List<PrimaryKeySchema> primaryKeySchemaList(Object... primaryKey) {
        if (primaryKey.length > 4) {
            logger.info("注意:表格存储主键最多可设置4个,可变参数个数最大为4");
            return null;
        }
        List<PrimaryKeySchema> primaryKeySchemaList = new ArrayList<>();
        for (Object PRIMARYKEY : primaryKey) {
            if (PRIMARYKEY instanceof String) {
                String PRIMARY_KEY = (String) PRIMARYKEY;
                primaryKeySchemaList.add(new PrimaryKeySchema(PRIMARY_KEY, PrimaryKeyType.STRING));
            }
        }
        return primaryKeySchemaList;
    }

    /**
     * 更新表
     *
     * @param timeToLive       数据过期时间 单位:秒 例如：一年 365*24*3600 -1表示永不过期
     * @param maxVersions      保存最大版本数 设置为3即代表每列最多保存 N 个最新的版本
     * @param maxTimeDeviation 有效版本偏差 [数据写入时间-有效版本偏差，数据写入时间+有效版本偏差)
     */
    public void updateTable(Integer timeToLive, Integer maxVersions, Long maxTimeDeviation, String TableName) {
        if (maxVersions == null || TableName == null) {
            return;
        }
        TableOptions tableOptions = new TableOptions();
        if (maxVersions != null) {
            tableOptions = new TableOptions(maxVersions);
        } else if (timeToLive != null) {
            tableOptions = new TableOptions(timeToLive, maxVersions);
        } else if (maxTimeDeviation != null) {
            tableOptions = new TableOptions(timeToLive, maxVersions, maxTimeDeviation);
        }
        UpdateTableRequest updateTableRequest = new UpdateTableRequest(TableName);
        updateTableRequest.setTableOptionsForUpdate(tableOptions);
        client.updateTable(updateTableRequest);
    }

    /**
     * 获取表相关信息
     */
    public void describeTable(String TableName) {
        DescribeTableRequest describeStreamRequest = new DescribeTableRequest(TableName);
        DescribeTableResponse describeTableResponse = client.describeTable(describeStreamRequest);
        TableMeta tableMeta = describeTableResponse.getTableMeta();
        List<PrimaryKeySchema> primaryKeySchemaList = tableMeta.getPrimaryKeyList();
        for (PrimaryKeySchema primaryKeySchema : primaryKeySchemaList) {
            logger.info("表:{}主键:{}", TableName, primaryKeySchema);
        }
        TableOptions tableOptions = describeTableResponse.getTableOptions();
        ReservedThroughputDetails reservedThroughputDetails = describeTableResponse.getReservedThroughputDetails();
        logger.info("表:{},数据过期时间timeToLive:{},最大版本数maxVersions:{},预留读吞吐量:{},预留写吞吐量:{}", TableName, tableOptions
                .getTimeToLive(), reservedThroughputDetails.getCapacityUnit().getReadCapacityUnit(),
                reservedThroughputDetails.getCapacityUnit().getWriteCapacityUnit());
    }

    /**
     * 删除表
     */
    public void deleteTable(String TableName) {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest(TableName);
        client.deleteTable(deleteTableRequest);
    }

}
