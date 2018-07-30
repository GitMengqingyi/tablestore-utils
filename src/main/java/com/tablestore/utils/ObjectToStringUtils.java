package com.tablestore.utils;
/**
 * This7 file created by mengqingyi on 2017-11-13.
 */

import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 类文件注释(Class file)
 *
 * @author mengqingyi
 * @classDescription 对Object转为表格存储所需的ColumnValue
 * @create 2017-11-13 16:25
 **/
public class ObjectToStringUtils {
    /**
     * 定义一个常量用来用于空值的保存
     */
    private static final ColumnValue COLUMN_VALUE = new ColumnValue("", ColumnType.STRING);

    public static ColumnValue objectToColumnValue(Object obj) {
        //空值使用默认
        if (obj == null) {
            return COLUMN_VALUE;
        }
        //字节使用string保存
        if (obj instanceof Byte) {
            String byteValue = String.valueOf(obj);
            return ColumnValue.fromString(byteValue);
        }
        //String类型使用String保存
        if (obj instanceof String) {
            String stringValue = String.valueOf(obj);
            return ColumnValue.fromString(stringValue);
        }
        //布尔类型使用Boolean保存
        if (obj instanceof Boolean) {
            Boolean booleanValue = (Boolean) obj;
            return ColumnValue.fromBoolean(booleanValue);
        }
        //Integer类型使用long保存
        if (obj instanceof Integer) {
            Integer integerValue = (Integer) obj;
            return ColumnValue.fromLong(integerValue);
        }
        //long类型使用long进行保存
        if (obj instanceof Long) {
            Long longValue = (Long) obj;
            return ColumnValue.fromLong(longValue);
        }
        //float类型直接使用fromDouble保存
        if (obj instanceof Float) {
            Float floatValue = (Float) obj;
            return ColumnValue.fromDouble(floatValue);
        }
        //double类型直接使用fromDouble保存
        if (obj instanceof Double) {
            Double doubleValue = (Double) obj;
            return ColumnValue.fromDouble(doubleValue);
        }
        //BigDecimal类型的，使用其toPlainString()方法对其转换，并以String进行保存
        if (obj instanceof BigDecimal) {
            BigDecimal bigDecimalValue = (BigDecimal) obj;
            return ColumnValue.fromString(bigDecimalValue.toPlainString());
        }
        //日期类型规范存储
        if (obj instanceof Date) {
            Date dateValue = (Date) obj;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String date;
            try {
                date = simpleDateFormat.format(dateValue);
            } catch (Exception e) {
                date = obj.toString();
                e.printStackTrace();
            }
            return ColumnValue.fromString(date);
        }
        //使用默认方法,如果是实体对象请重写 toString()方法
        return ColumnValue.fromString(obj.toString());
    }
}
