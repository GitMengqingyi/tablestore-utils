package com.tablestore.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 使用gzip进行字符串压缩的通用工具类
 *
 * @author mengqingyi
 * @create 2017-10-31 19:53
 **/
public class GZipUtils {
    public static final int BUFFER = 1024;
    private static void compress(InputStream inputStream, OutputStream outputStream) throws IOException {
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        int count;
        byte[] data = new byte[BUFFER];
        while ((count = inputStream.read(data, 0, BUFFER)) != -1) {
            gzipOutputStream.write(data, 0, count);
        }
        gzipOutputStream.finish();
        gzipOutputStream.flush();
        gzipOutputStream.close();
    }

    private static byte[] compress(byte[] data) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        //压缩
        compress(byteArrayInputStream, byteArrayOutputStream);
        byte[] output = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.flush();
        byteArrayOutputStream.close();
        byteArrayInputStream.close();
        return output;
    }

    private static byte[] decompress(byte[] data) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        //解压缩
        decompress(byteArrayInputStream, byteArrayOutputStream);
        data = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.flush();
        byteArrayOutputStream.close();
        byteArrayInputStream.close();
        return data;
    }

    private static void decompress(ByteArrayInputStream byteArrayInputStream,
            ByteArrayOutputStream byteArrayOutputStream) throws IOException {
        GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
        int count;
        byte[] data = new byte[BUFFER];
        while ((count = gzipInputStream.read(data, 0, BUFFER)) != -1) {
            byteArrayOutputStream.write(data, 0, count);
        }
        gzipInputStream.close();
    }
}
