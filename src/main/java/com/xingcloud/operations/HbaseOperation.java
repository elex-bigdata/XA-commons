package com.xingcloud.operations;

import com.xingcloud.operations.utils.ConfigReader;
import com.xingcloud.operations.utils.Constants;
import com.xingcloud.operations.utils.Dom;
import com.xingcloud.operations.utils.Log4jProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wanghaixing on 15-7-23.
 */
public class HbaseOperation {
    private static final Log LOG = LogFactory.getLog(HbaseOperation.class);

    private Map<String, List<String>> time_pros = new HashMap<String, List<String>>();

    HbaseOperation() {
        init();
    }

    private void init() {
        Log4jProperties.init();
        Dom dom = ConfigReader.getDom("projects_conf.xml");
        List<Dom> projectsDom = dom.elements("projects");
        for (Dom proDom : projectsDom) {
            String keepTime = proDom.elementText("keep_time");
            String[] pros = proDom.elementText("pro").split(",");
            List proList = Arrays.asList(pros);
            time_pros.put(keepTime, proList);
        }
    }

    public static void main(String[] args) {
        HbaseOperation ho = new HbaseOperation();
        List<String> proList = ho.time_pros.get("3");

        for(String pid : proList) {
            System.out.println(pid);
        }

        /*ExecutorService service = Executors.newFixedThreadPool(16);
        for(int n = 0 ; n < 16; n++) {
            service.execute(new AlterTable("node" + n, proList, Constants.SET_TTL));
        }*/
    }

    /*public void setTTL(List<String> proList, String op) {
        ExecutorService service = Executors.newFixedThreadPool(16);
        for(int n = 0 ; n < 16; n++) {
            service.execute(new AlterTable('node' + n, proList));
        }
    }

    public void alterTable(List<String> proList) {
        ExecutorService service = Executors.newFixedThreadPool(16);
        for(int n = 0 ; n < 16; n++) {
            service.execute(new AlterTable('node' + n, proList));
        }
    }*/
}

class AlterTable implements Runnable {
    private static final Log LOG = LogFactory.getLog(AlterTable.class);

    String node;
    List<String> proList;
    String op;

    AlterTable(String node, List<String> proList, String op) {
        this.node = node;
        this.proList = proList;
        this.op = op;
    }

    @Override
    public void run() {
        LOG.info("begin process hbase records on " + node + "...");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", node);
        conf.set("hbase.zookeeper.property.clientPort", "3181");
        String tableName = "";

        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            HTableDescriptor tableDescripter = null;
            if (op.equals(Constants.SET_TTL)) {
                for(String pro : proList) {
                    tableName = "deu_" + pro;
                    LOG.info("begin set table TTL:  " + tableName + "  on " + node);
                    admin.disableTable(tableName);
                    HColumnDescriptor family = new HColumnDescriptor(Constants.COLUMNFAMILY);
                    family.setTimeToLive(Constants.THREE_MONTH);
                    admin.enableTable(tableName);
                }
            } else if (op.equals(Constants.DELETE)) {
                for(String pro : proList) {
                    tableName = "deu_" + pro;
                    LOG.info("begin clear table data:  " + tableName + "  on " + node);
                    tableDescripter = admin.getTableDescriptor(Bytes.toBytes(tableName));
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                    admin.createTable(tableDescripter);
                }
            }
            admin.close();

        } catch (IOException e) {
            LOG.error("process table: " + tableName + " failed on " + node);
            LOG.error(e.getMessage());
        }
        LOG.info("end process hbase records on " + node + "...");
    }
}

