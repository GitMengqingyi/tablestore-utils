package com.tablestore.utils;
/**
 * This file created by mengqingyi on 2017-11-10.
 */


import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 类文件注释(Class file)
 *
 * @author mengqingyi
 * @classDescription Gzip压缩，运用jdk自带的GZIPInputStream和GZIPOutputStream完成压缩解压缩
 * @create 2017-11-10 13:28
 **/
public class GzipUtil {

    public static String compress(String str) {
        if (StringUtils.isBlank(str)) {
            return str;
        }
        //创建一个新的输出流
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 使用默认缓冲区大小创建新的输出流
        GZIPOutputStream gzip;
        // 将字节写入此输出流
        try {
            gzip = new GZIPOutputStream(out);
            //指定一个字符集
            gzip.write(str.getBytes("utf-8"));
            gzip.close();
            // 使用指定的 charsetName，通过解码字节将缓冲区内容转换为字符串
            return out.toString("ISO-8859-1");
        } catch (IOException e) {
            e.printStackTrace();
            return str;
        }
    }

    public static String unCompress(String str) throws IOException {
        if (StringUtils.isBlank(str)) {
            return str;
        }
        // 创建一个新的输出流
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 创建一个 ByteArrayInputStream，使用 buf 作为其缓冲区数组
        ByteArrayInputStream in = new ByteArrayInputStream(str.getBytes("ISO-8859-1"));
        // 使用默认缓冲区大小创建新的输入流
        GZIPInputStream gzip = new GZIPInputStream(in);
        byte[] buffer = new byte[256];
        int n = 0;
        // 将未压缩数据读入字节数组
        while ((n = gzip.read(buffer)) >= 0) {
            out.write(buffer, 0, n);
        }
        // 使用指定的 charsetName，通过解码字节将缓冲区内容转换为字符串
        return out.toString("utf-8");
    }
}
