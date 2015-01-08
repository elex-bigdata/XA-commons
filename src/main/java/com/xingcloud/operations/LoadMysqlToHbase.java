package com.xingcloud.operations;

import com.xingcloud.mysql.MySqlResourceManager;
import com.xingcloud.mysql.UpdateFunc;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.operations.utils.Constants;
import com.xingcloud.operations.utils.Log4jProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wanghaixing on 15-1-6.
 */
public class LoadMysqlToHbase {
    private static final Log LOG = LogFactory.getLog(LoadMysqlToHbase.class);

    public static void main(String[] args) throws Exception{
        Log4jProperties.init();
        LoadMysqlToHbase lmth = new LoadMysqlToHbase();
//        List<String> projects = lmth.getAllProjects();
        List<String> projects = new ArrayList<String>();
        projects.add("v9");
        String cmd = args[0];
        if (cmd.equals("load")) {
            lmth.load(projects);
        } else if(cmd.equals("test")) {
            lmth.test();
        }

    }

    public void test() throws Exception{
        String node = InetAddress.getLocalHost().getHostAddress();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", node);
        conf.set("hbase.zookeeper.property.clientPort", Constants.HBASE_PORT);
        HTable table = new HTable(conf, Constants.ATTRIBUTE_TABLE);
        Scan scan = new Scan();
        int pidDict = Constants.dict.getPidDict("v9");
        int attrDict = Constants.dict.getAttributeDict("browser");
        byte[] startKey = Bytes.add(Bytes.toBytes(pidDict), Bytes.toBytes(attrDict));
        byte[] endKey = Bytes.add(Bytes.toBytes(pidDict), Bytes.toBytes(attrDict + 1));
        scan.setStartRow(startKey);
        scan.setStopRow(endKey);
        scan.addColumn(Bytes.toBytes(Constants.USER_COLUMNFAMILY), Bytes.toBytes(Constants.USER_QUALIFIER));
        ResultScanner scanner = table.getScanner(scan);
        for(Result r : scanner) {
            byte[] rowkey = r.getRow();
            long uid = Bytes.toLong(Bytes.tail(rowkey, 8));
            System.out.println(String.valueOf(uid) + "\t" + Bytes.toString(r.getValue(Bytes.toBytes(Constants.USER_COLUMNFAMILY), Bytes.toBytes(Constants.USER_QUALIFIER))));       // + "\t" + Bytes.toLong(r.getValue(columnfamily, qualifier))
        }
    }

    public void load(List<String> projects) {
        ExecutorService service = Executors.newFixedThreadPool(10);
        for(String pro : projects) {
            DumpWorker dumpWorker = new DumpWorker(pro);
            service.submit(dumpWorker);
        }
        service.shutdown();
    }

    public List<String> getAllProjects() {
        List<String> projects = new ArrayList<String>();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            String sql = "show databases";
            conn = MySqlResourceManager.getInstance().getConnLocalNode();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            String proj = "";
            while(rs.next()) {
                proj = rs.getString(1);
                if(proj.startsWith("16_")) {
                    projects.add(proj);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                stmt.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return projects;
    }



    private byte[] createRowKey(String pid, String attr, long uid) throws Exception {
        int pidDict = Constants.dict.getPidDict(pid);
        int attrDict = Constants.dict.getAttributeDict(attr);
        return Bytes.add(Bytes.toBytes(pidDict), Bytes.toBytes(attrDict), Bytes.toBytes(uid));
    }

    public Map<String, UserProp> getUserPropMap(String pid) throws SQLException {
        Map<String, UserProp> upMap = new HashMap<String, UserProp>();
        List<UserProp> userProps = MySqlResourceManager.getInstance().getUserPropsFromLocal(pid);
        for(UserProp up : userProps) {
            String prop_name = up.getPropName();
            if(upMap.get(prop_name) == null) {
                upMap.put(prop_name, up);
            }
        }

        return upMap;
    }

    public void loadToHBase(String fileName, String pid, UserProp userProp) throws Exception {
        LOG.info("read file :   " + fileName);
        long currentTime = System.currentTimeMillis();

        /*String[] dirs = fileName.split("/");
        int len = dirs.length;
        String pid = dirs[len-2];
        String attr = dirs[len-1];
        int attr_len = attr.length();
        attr = attr.substring(0, attr_len-4);*/

        String record = "";
        BufferedReader br = new BufferedReader(new FileReader(new File(fileName)));
        String[] items = null;
        byte[] rowkey = null;

//        Map<String, UserProp> userPropMap = getUserPropMap(pid);
//        UserProp userProp = userPropMap.get(attr);

        String attr = userProp.getPropName();
        LOG.info("Begin to load table : 16_" + pid + "." + attr + " to hbase...");
        String node = InetAddress.getLocalHost().getHostAddress();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", node);
        conf.set("hbase.zookeeper.property.clientPort", Constants.HBASE_PORT);
        HTable table = new HTable(conf, Constants.ATTRIBUTE_TABLE);
        table.setAutoFlush(false);
        table.setWriteBufferSize(Constants.WRITE_BUFFER_SIZE);
        List<Put> puts = new ArrayList<Put>();

        Map<byte[], String> users = new HashMap<byte[], String>();

        while((record = br.readLine()) != null) {
//            System.out.println("--------------------------" + record);
            items = record.split("\t");
            if(items != null && items.length ==2) {
                rowkey = createRowKey(pid, attr, Long.parseLong(items[0]));
                users.put(rowkey, items[1]);

                Put put = new Put(rowkey);
                put.setWriteToWAL(Constants.TableWalSwitch);
                byte[] value = null;
                switch (userProp.getPropType()) {
                    case sql_bigint:
                        value = Bytes.toBytes(Long.parseLong(items[1]));
                        break;
                    case sql_datetime:
                        value = Bytes.toBytes(Long.parseLong(items[1]));
                        break;
                    case sql_string:
                        value = Bytes.toBytes(items[1]);
                }

                put.add(Constants.USER_COLUMNFAMILY.getBytes(), Constants.USER_QUALIFIER.getBytes(), value);
                puts.add(put);
            }
        }

        if (userProp.getPropFunc() == UpdateFunc.cover) {
            table.put(puts);
        } else if (userProp.getPropFunc() == UpdateFunc.once) {
            for (Put put : puts) {
                table.checkAndPut(put.getRow(), Constants.USER_COLUMNFAMILY.getBytes(), Constants.USER_QUALIFIER.getBytes(), null, put);
            }
        } else if (userProp.getPropFunc() == UpdateFunc.inc) {
            for (Map.Entry<byte[], String> user : users.entrySet()) {
                table.incrementColumnValue(user.getKey(), Constants.USER_COLUMNFAMILY.getBytes(), Constants.USER_QUALIFIER.getBytes(), Long.parseLong(user.getValue()), false);
            }
        }
        LOG.info("load table : 16_" + pid + "." + attr + " to hbase using " + (System.currentTimeMillis() - currentTime) + "ms");

    }

    class DumpWorker implements Runnable {
        private final Log LOG = LogFactory.getLog(DumpWorker.class);
        private String project;
        private String fileName;

        public DumpWorker(String project) {
            this.project = project;
        }

        @Override
        public void run() {
            LOG.info("Begin to dump and load database: 16_" + project);
            long t1 = System.currentTimeMillis();
            try {
                String des = Constants.local_path_mysql_dump + project + "/";
                File dir = new File(des);
                if(!dir.exists()) {
                    dir.mkdir();
                    Runtime.getRuntime().exec("sudo chmod 777 " + des);
                }
                String dump_command = "";
                List<UserProp> userProps = MySqlResourceManager.getInstance().getUserPropsFromLocal(project);
                for(UserProp up : userProps) {
                    System.out.println("table name-----------" + up.getPropName());
                    Runtime rt = Runtime.getRuntime();
                    dump_command = "mysqldump -uxingyun -pOhth3cha --quick --single-transaction -t --databases 16_" + project + " --tables " + up.getPropName() + " --tab=" + des;
                    String[] cmds = new String[]{"/bin/sh", "-c", dump_command};
                    Process process = rt.exec(cmds);
                    int result = process.waitFor();
                    if (result != 0)
                        throw new RuntimeException("ERROR !!!! dump table " + up.getPropName() + " for " + project + " failed.");

                    fileName = des + up.getPropName() + ".txt";
                    System.out.println("table file name-----------" + fileName);
//                    fileName = des + up.getPropName() + ".txt";
                    loadToHBase(fileName, project, up);
                }

                LOG.info("End to dump and load database: 16_" + project + ". Using " + (System.currentTimeMillis() - t1) + "ms");

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        public String getProject() {
            return project;
        }
    }
}
