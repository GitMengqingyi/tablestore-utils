package com.tablestore.utils;
/**
 * This file created by mengqingyi on 2017-11-16.
 */

import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;

/**
 * 类文件注释(Class file)
 *
 * @author mengqingyi
 * @classDescription 根据列类型对列值进行回溯
 * @create 2017-11-16 16:40
 **/
public class ColumnValueToObjectUtils {
    public static Object toConvert(ColumnValue columnValue) {
        //健壮性判断，不过一般来说此处值不可能为空
        if (columnValue == null) {
            return "";
        }
        Object value = columnValue.toString();
        if (ColumnType.STRING == columnValue.getType()) {
            value = columnValue.asString();
        }
        if (ColumnType.BOOLEAN == columnValue.getType()) {
            value = columnValue.asBoolean();
        }
        //数字类型的转换？
        if (ColumnType.INTEGER == columnValue.getType()) {
            value = columnValue.asLong();
        }
        if (ColumnType.DOUBLE == columnValue.getType()) {
            columnValue.asDouble();
        }
        return value;
    }
}
