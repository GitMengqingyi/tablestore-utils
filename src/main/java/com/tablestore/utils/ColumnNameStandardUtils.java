package com.tablestore.utils;
/**
 * This file created by mengqingyi on 2017-11-14.
 */

import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 类文件注释(Class file)
 *
 * @author mengqingyi
 * @classDescription 表名, 列名规范化工具
 * @create 2017-11-14 10:20
 **/
public class ColumnNameStandardUtils {
    private static final String REGEX = "^[a-zA-Z][a-zA-Z0-9_ ]+$";
    private static final int MAX_TABLE_NAME_LENGTH = 255;

    /**
     * 1.必须由英文字母、数字或下划线（_）组成 2.首字符必须为英文字母或下划线（_） 3.大小写敏感 4.长度在 1~255 个字符之间
     */
    public static String standard(String columnName) {
        if (StringUtils.isBlank(columnName)) {
            //如果列名为空,则取当前时间拼接表名。此处需要sleep线程,防止同一时间多次调用此方法,导致同表列明重复？
            columnName = "column_" + Instant.now().getEpochSecond();
            // try {
            //     Thread.sleep(1);
            // } catch (InterruptedException e) {
            //     e.printStackTrace();
            // }
        }
        if (!match(columnName)) {
            //如果不符合首字母非数字或者存在除字母数字下划线之后的列名,移除使其符合规范
            columnName = columnName.replaceAll(REGEX, columnName);
        }
        //表格存储名称超过255字符长度,截取255位
        if (columnName.length() > MAX_TABLE_NAME_LENGTH) {
            columnName = columnName.substring(0, MAX_TABLE_NAME_LENGTH - 1);
        }
        return columnName;
    }

    /**
     * 正则表达式 判断列名是否由首字母为英文字母且只能包含字母、数字、下划线
     */
    private static Boolean match(String columnName) {
        Pattern pattern = Pattern.compile(REGEX);
        Matcher matcher = pattern.matcher(columnName);
        return matcher.matches();
    }
}
