package com.xingcloud.operations;

import com.xingcloud.operations.utils.Log4jProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by wanghaixing on 15-2-5.
 */

public class DeleteRecFromHbase {

    private static final Log LOG = LogFactory.getLog(DeleteRecFromHbase.class);
    private static boolean maxVersion = false;

    public static void main(String[] args) throws Exception{
        Log4jProperties.init();
        String pid = args[0];
        String startKey = args[1];
        String endKey = args[2];
        if(args.length == 4 && "true".equals(args[3])){
            maxVersion = true;
        }

        ExecutorService service = Executors.newFixedThreadPool(16);
        DeleteRecFromHbase drh = new DeleteRecFromHbase();
        for(int i=0;i<16;i++){
            service.execute(new Deleter("node" + i, pid, startKey, endKey, drh, true));
        }
        service.shutdownNow();

    }

}



class Deleter implements Runnable {
    private static final Log LOG = LogFactory.getLog(Deleter.class);
    String node;
    String tableName;
    String startKey;
    String endKey;
    DeleteRecFromHbase query;
    boolean maxVersion = false;

    public Deleter(String node,String tableName,String startKey,String endKey,DeleteRecFromHbase query,boolean maxVersion){
        this.node = node;
        this.tableName = tableName;
        this.startKey = startKey;
        this.endKey = endKey;
        this.query = query;
        this.maxVersion = maxVersion;
    }

    @Override
    public void run() {
        LOG.info("begin delete hbase records on " + node + "...");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", node);
        conf.set("hbase.zookeeper.property.clientPort", "3181");

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(endKey));
        if(maxVersion){
            scan.setMaxVersions();
        }else{
            scan.setMaxVersions(1);
        }
        scan.setFilter(new KeyOnlyFilter());
        scan.setCaching(10000);

        HTable table = null;
        ResultScanner scanner = null;
        long count = 0l;
        try {
            table = new HTable(conf,"deu_" + tableName);
            scanner = table.getScanner(scan);
            List<Delete> deletes = new ArrayList<Delete>();

            for(Result r : scanner){
                deletes.add(new Delete(r.getRow()));
                count++;
            }
            table.delete(deletes);
            LOG.info("end delete hbase records on " + node + ". " + count + " rows deleted...");
        } catch (IOException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        } finally {
            scanner.close();
            try {
                table.close();
            } catch (IOException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            }

        }

    }
}


