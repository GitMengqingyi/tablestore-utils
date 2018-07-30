package com.tablestore.utils;
/**
 * This file created by mengqingyi on 2017-11-14.
 */

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.WideColumnIterator;
import com.alicloud.openservices.tablestore.model.filter.ColumnPaginationFilter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.util.HashMap;

/**
 * 类文件注释(Class file)
 *
 * @author mengqingyi
 * @classDescription 宽行读取工具类
 * @create 2017-11-14 9:55
 **/
@Repository
public class WideRowsQueryUtils {
    @Autowired
    @Qualifier("createClient")
    private SyncClient client;
    private static final int LIMIT = 10;
    private static final int INITIAL_CAPACITY = 16;

    /**
     * 使用startColumn和endColumn读取一定范围的属性列 适用于已知 列名且按照列保存 start和end指定列左闭右开[)
     */
    public HashMap rangeQuery(String tableName, PrimaryKey primaryKey, String startColumn, String endColumn) {
        HashMap<String, Object> hashMap = new HashMap<>(INITIAL_CAPACITY);
        //读取一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        //设置读取列的范围
        criteria.setStartColumn(startColumn);
        criteria.setEndColumn(endColumn);
        //设置读取版本
        criteria.setMaxVersions(1);
        GetRowResponse rowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = rowResponse.getRow();
        if (row != null) {
            Column[] columns = row.getColumns();
            for (Column column : columns) {
                //填装key/value到hashMap中
                hashMap.put(column.getName(), column.getValue());
            }
        }
        return hashMap;
    }

    /**
     * 使用ColumnPaginationFilter配合startColumn参数 ColumnPaginationFilter中有limit和offset两个参数,配合startColumn
     * 会从startColumn开始读，跳过offset个属性列,读取limit个属性列。 应用场景:分页读取属性列
     */
    public HashMap pageQuery(String tableName, PrimaryKey primaryKey, String startColumn, int offset) {
        HashMap<String, Object> hashMap = new HashMap<>(INITIAL_CAPACITY);
        //读取一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        //设置从哪一行开始读
        criteria.setStartColumn(startColumn);
        //使用ColumnPaginationFilter设置一次要读取的列数
        criteria.setFilter(new ColumnPaginationFilter(LIMIT, offset));
        //设置读取版本
        criteria.setMaxVersions(1);
        GetRowResponse rowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = rowResponse.getRow();
        if (row != null) {
            Column[] columns = row.getColumns();
            for (Column column : columns) {
                //填装key/value到hashMap中
                hashMap.put(column.getName(), column.getValue());
            }
        }
        return hashMap;
    }

    /**
     * 宽行读取，指定每次读取多少列
     */
    public HashMap pageQuery(String tableName, PrimaryKey primaryKey, String startColumn, int limit, int offset) {
        HashMap<String, Object> hashMap = new HashMap<>(INITIAL_CAPACITY);
        //读取一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        //设置从哪一行开始读
        criteria.setStartColumn(startColumn);
        //使用ColumnPaginationFilter设置一次要读取的列数
        criteria.setFilter(new ColumnPaginationFilter(limit, offset));
        //设置读取版本
        criteria.setMaxVersions(1);
        GetRowResponse rowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = rowResponse.getRow();
        if (row != null) {
            Column[] columns = row.getColumns();
            for (Column column : columns) {
                //填装key/value到hashMap中
                hashMap.put(column.getName(), column.getValue());
            }
        }
        return hashMap;
    }

    /**
     * 使用WideColumnIterator,迭代读取一行中的而所有属性列
     */
    public HashMap iteratorQuery(String tableName, PrimaryKey primaryKey) {
        HashMap<String, Object> hashMap = new HashMap<>(INITIAL_CAPACITY);
        //读取一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        //设置读取版本
        criteria.setMaxVersions(1);
        //配置宽行读
        WideColumnIterator wideColumnIterator = client.createWideColumnIterator(new GetRowRequest(criteria));
        while (wideColumnIterator.hasNextColumn()) {
            Column column = wideColumnIterator.nextColumn();
            //填装key/value到hashMap中
            hashMap.put(column.getName(), column.getValue());
        }
        return hashMap;
    }


}
