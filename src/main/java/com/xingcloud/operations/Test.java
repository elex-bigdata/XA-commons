package com.xingcloud.operations;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

/**
 * Created by wanghaixing on 15-5-7.
 */
public class Test {
    public static final String HDFS_PATH = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/stream_log/pid/2015-05-06/22apple";

    static {
        //让URL识别hdfs协议
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws Exception {
        URL url = new URL(HDFS_PATH);
        InputStream in = url.openStream();
//        OutputStream out =

        IOUtils.copyBytes(in, System.out, 1024, true);
    }
}
