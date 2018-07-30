package com.tablestore.service;
/**
 * This file created by mengqingyi on 2017-11-14.
 */

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RangeIteratorParameter;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.WideColumnIterator;
import com.alicloud.openservices.tablestore.model.filter.ColumnPaginationFilter;
import com.tablestore.constant.CommonTableNameEnum;
import com.tablestore.utils.ColumnValueToObjectUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * 类文件注释(Class file)
 *
 * @author mengqingyi
 * @classDescription 通用表格存储查询, 包含查询单列和宽行查询两大类
 * @create 2017-11-14 13:47
 **/
@SuppressWarnings("rawtypes")
@Repository
public class CommonTableStoreQuery {
    @Autowired
    @Qualifier("createClient")
    private SyncClient client;
    private static final int INITIAL_CAPACITY = 16;
    private static final int INITIAL_LIMIT = 10;
    private static final Logger logger = LoggerFactory.getLogger("log.tableStore.CommonTableStoreQuery");

    /**
     * 查询某表单列内容,查询结果封装在hashMap中,key为列名 value为列值
     */
	public HashMap querySingleRow(PrimaryKey primaryKey, CommonTableNameEnum commonTableNameEnum, String columnName) {
        String tableName = commonTableNameEnum.getName();
        HashMap<String, Object> hashMap = new HashMap<>(INITIAL_CAPACITY);
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        criteria.setMaxVersions(1);
        GetRowRequest rowRequest = new GetRowRequest(criteria);
        GetRowResponse rowResponse = client.getRow(rowRequest);
        Row row = rowResponse.getRow();
        if (row != null) {
            List<Column> columns = row.getColumn(columnName);
            for (Column column : columns) {
                hashMap.put(column.getName(), column.getValue().toString());
            }
        }
        return hashMap;
    }

    /**
     * 将需要查的列使用可变参数传递,查询这些列的内容,key为列名 value为列值
     */
    public HashMap querySingleRows(PrimaryKey primaryKey, CommonTableNameEnum commonTableNameEnum,
            String... columnNames) {
        String tableName = commonTableNameEnum.getName();
        HashMap<String, Object> hashMap = new HashMap<>(INITIAL_CAPACITY);
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        criteria.setMaxVersions(1);
        GetRowRequest rowRequest = new GetRowRequest(criteria);
        GetRowResponse rowResponse = client.getRow(rowRequest);
        Row row = rowResponse.getRow();
        if (row != null) {
            for (String columnName : columnNames) {
                List<Column> columns = row.getColumn(columnName);
                for (Column column : columns) {
                    hashMap.put(column.getName(), column.getValue().toString());
                }
            }
        }
        return hashMap;
    }

    /**
     * 宽行读取 按照已知有序列名排序的属性列,注意指定的startColumn和endColumn列为左闭右开[)
     */
    public HashMap wideRangeQuery(PrimaryKey primaryKey, CommonTableNameEnum commonTableNameEnum, String startColumn,
            String endColumn) {
        String tableName = commonTableNameEnum.getName();
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
    public HashMap widePageQuery(PrimaryKey primaryKey, CommonTableNameEnum commonTableNameEnum, String startColumn,
            int offset) {
        String tableName = commonTableNameEnum.getName();
        HashMap<String, Object> hashMap = new HashMap<>(INITIAL_CAPACITY);
        //读取一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        //设置从哪一行开始读
        criteria.setStartColumn(startColumn);
        //使用ColumnPaginationFilter设置一次要读取的列数
        criteria.setFilter(new ColumnPaginationFilter(INITIAL_LIMIT, offset));
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
    public HashMap wideIteratorQuery(PrimaryKey primaryKey, CommonTableNameEnum commonTableNameEnum) {
        String tableName = commonTableNameEnum.getName();
        HashMap<String, Object> hashMap = new HashMap<>(INITIAL_CAPACITY);
        //读取一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        //设置读取版本
        criteria.setMaxVersions(1);
        //配置宽行读
        WideColumnIterator wideColumnIterator = client.createWideColumnIterator(new GetRowRequest(criteria));
        if (wideColumnIterator == null) {
            logger.warn("[CommonTableStoreQuery查询]wideIteratorQuery结果集为空,表格存储中不存在该查询值");
            return null;
        }
        while (wideColumnIterator.hasNextColumn()) {
            Column column = wideColumnIterator.nextColumn();
            //填装key/value到hashMap中
            hashMap.put(column.getName(), column.getValue());
        }
        return hashMap;
    }

    /**
     * 范围读  效率比迭代读低一点，耗时更久(小数据量测试,并不代表真实环境) 由于绝大部分 分区键被MD5散裂化，因而无法使用该范围读，如有需求，请自己使用for循环进行迭代读去 本方法适用于同一用户多条记录的查询
     */
    public List<HashMap<String, Object>> getRange(CommonTableNameEnum commonTableNameEnum, PrimaryKey startPKValue,
            PrimaryKey endPKValue) {
        String tableName = commonTableNameEnum.getName();
        ArrayList<HashMap<String, Object>> list = Lists.newArrayList();
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        //装填 起始查询主键
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPKValue);
        //装填 结束查询主键
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPKValue);
        //查询最新一条记录
        rangeRowQueryCriteria.setMaxVersions(1);
        //创建查询请求
        GetRangeRequest getRangeRequest = new GetRangeRequest(rangeRowQueryCriteria);
        //创建返回体
        GetRangeResponse getRowResponse = client.getRange(getRangeRequest);
        List<Row> rows = getRowResponse.getRows();
        //对结果进行遍历
        for (Row row : rows) {
            Column[] columns = row.getColumns();
            HashMap<String, Object> hashMap = Maps.newHashMap();
            for (Column column : columns) {
                String name = column.getName();
                ColumnValue columnValue = column.getValue();
                Object value = ColumnValueToObjectUtils.toConvert(columnValue);
                hashMap.put(name, value);
            }
            //判断是否还有下一个值
            PrimaryKey nextStartPrimaryKey = getRowResponse.getNextStartPrimaryKey();
            if (nextStartPrimaryKey != null) {
                rangeRowQueryCriteria.setInclusiveStartPrimaryKey(nextStartPrimaryKey);
            }
            list.add(hashMap);
        }
        return list;
    }

    /**
     * 迭代读 效率略高，耗时短(小数据量测试,并不代表真实环境) 由于绝大部分 分区键被MD5散裂化，因而无法使用该范围读，如有需求，请自己使用for循环进行迭代读去 本方法适用于 一个用户 多条记录的查询
     */
    public List<HashMap<String, Object>> getRangeByIterator(CommonTableNameEnum commonTableNameEnum,
            PrimaryKey startPKValue, PrimaryKey endPKValue) {
        String tableName = commonTableNameEnum.getName();
        ArrayList<HashMap<String, Object>> list = Lists.newArrayList();
        RangeIteratorParameter rangeIteratorParameter = new RangeIteratorParameter(tableName);
        //装填 起始查询主键
        rangeIteratorParameter.setInclusiveStartPrimaryKey(startPKValue);
        //装填 结束查询主键
        rangeIteratorParameter.setExclusiveEndPrimaryKey(endPKValue);
        //查询最新一条记录
        rangeIteratorParameter.setMaxVersions(1);
        //获取查询结果集
        Iterator<Row> iterator = client.createRangeIterator(rangeIteratorParameter);
        if (iterator == null) {
            return null;
        }
        while (iterator.hasNext()) {
            Row row = iterator.next();
            Column[] columns = row.getColumns();
            HashMap<String, Object> hashMap = Maps.newHashMap();
            for (Column column : columns) {
                String name = column.getName();
                ColumnValue columnValue = column.getValue();
                Object value = ColumnValueToObjectUtils.toConvert(columnValue);
                hashMap.put(name, value);
            }
            list.add(hashMap);
        }
        return list;
    }
}
