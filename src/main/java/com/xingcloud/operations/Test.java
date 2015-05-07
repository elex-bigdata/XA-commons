package com.xingcloud.operations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;

/**
 * Created by wanghaixing on 15-5-7.
 */
public class Test {
    public static String HDFS_PATH = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/stream_log/pid/2015-05-06/22apple/stream_00000.log";

    public static void main(String[] args) throws Exception {
        System.out.println("helllllllllll");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(HDFS_PATH), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(HDFS_PATH));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
