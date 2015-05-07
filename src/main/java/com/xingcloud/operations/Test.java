package com.xingcloud.operations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
    public static String FIX_PATH = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/stream_log/pid/2015-05-06/22apple/";

    public static void main(String[] args) throws Exception {
        System.out.println("helllllllllll");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(FIX_PATH), conf);
        InputStream in = null;
        int count = 0;
        try {
            for(FileStatus fileStatus: fs.listStatus(new Path(FIX_PATH))){
                if(fileStatus.isFile() && count < 5) {
                    Path path = fileStatus.getPath();
                    in = fs.open(path);
                    IOUtils.copyBytes(in, System.out, 4096, false);
                } else {
                    break;
                }
            }
        } finally {
            IOUtils.closeStream(in);
        }

    }
}
