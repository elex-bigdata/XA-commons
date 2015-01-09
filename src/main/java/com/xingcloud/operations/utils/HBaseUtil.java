package com.xingcloud.operations.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class HBaseUtil {
    private static final Log LOG = LogFactory.getLog(HBaseUtil.class);

    private static final int max_size = 200;
    private static HTablePool pool;

    public static void init() {
        String node = null;
        try {
            node = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOG.error("cannot get the local node's host!!!");
            e.printStackTrace();
        }
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", node);
        conf.set("hbase.zookeeper.property.clientPort", Constants.HBASE_PORT);
        pool = new HTablePool(conf, max_size);
    }

    public static HTableInterface getHTable(String tableName) throws IOException {
        try {
            return pool.getTable(tableName);
        } catch (Exception e) {
            throw new IOException("Cannot get htable for table: " + tableName + "...", e);
        }
    }

    public static void closeAll() throws IOException {
        pool.close();
    }

}
