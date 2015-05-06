package com.xingcloud.operations;

import com.xingcloud.id.s.MySQLResourceManager;
import com.xingcloud.operations.utils.Constants;
import com.xingcloud.operations.utils.DateManager;
import com.xingcloud.uidtransform.HbaseMysqlUIDTruncator;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wanghaixing on 15-2-28.
 */
public class IdMapOperation {
    private static final Log LOG = LogFactory.getLog(IdMapOperation.class);

    private String date;
    public static int THREAD_NUM = 1;

    public static void main(String[] args) throws SQLException {
        String date = args[0];
        if (date == null || date.equals("")) {
            date = DateManager.getDaysBefore(0, 0);
        }

        IdMapOperation imo = new IdMapOperation();
        imo.clearOldData(date);
    }

    public void clearOldData(String date) throws SQLException {
        //要定期删除的项目
        String[] pids = new String[]{"sof-wpm", "sof-zip", "sof-windowspm", "quick-start","sof-ient", "sof-newgdp", "sof-newgdppop", "sof-yacnvd",
                "i18n-status", "web337", "lightning-speedial", "sof-dsk", "sof-installer", "omiga-plus", "webssearches", "sweet-page", "infospace"};

        THREAD_NUM = pids.length;
        ExecutorService service = Executors.newFixedThreadPool(THREAD_NUM);
        for(String pid : pids){
            service.submit(new IdMapExecutor(pid, date));
        }

        service.shutdown();
        LOG.info(" All finished " );
    }

    //  /data2/deleted/web337/2015-02-28.txt
    class IdMapExecutor implements Runnable {
        private String pid;
        private String fileName;

        public IdMapExecutor(String pid, String date) {
            this.pid = pid;
            this.fileName = Constants.deleted_uids_path + pid + "/" + date + ".txt";
        }

        @Override
        public void run() {
            LOG.info(" begin delete " + pid );
            long begin = System.currentTimeMillis();
            Connection conn = null;
            Statement statement = null;
            List<String> uids = readFromFile(fileName);
            List<String> ids = translate(uids);
            try {
                List<String> sqls = new ArrayList<String>();
                StringBuilder uidSql = null;
                for(int i = 0; i < ids.size(); i++){
                    if(i % 10000 == 0){
                        if(uidSql != null){
                            sqls.add(uidSql.toString());
                        }
                        uidSql = new StringBuilder(ids.get(i));
                    }else {
                        uidSql.append(",").append(ids.get(i));
                    }
                }
                sqls.add(uidSql.toString());

                conn = MySQLResourceManager.getInstance().getConnection();
                statement = conn.createStatement();
                int i = 0;
                for(String batch : sqls) {
                    String sql = "delete from vf_" + pid + ".id_map where id in (" + batch + ")";
                    statement.execute(sql);
                    LOG.info("delete batch " + i + " ...");
                    i++;
                }
                LOG.info(" delete " + pid + " finished cost " + (System.currentTimeMillis() - begin) + "ms");

            } catch (SQLException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            } finally {
                DbUtils.closeQuietly(statement);
                DbUtils.closeQuietly(conn);
            }

        }

        public List<String> translate(List<String> uids) {
            List<String> truncUids = null;
            try {
                truncUids = new ArrayList<String>();
                for(String uid : uids) {
                    long ul = Long.parseLong(uid);
                    truncUids.add(String.valueOf(HbaseMysqlUIDTruncator.truncate(ul)[0]));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return truncUids;
        }

        public List<String> readFromFile(String fileName) {
            List<String> uids = new ArrayList<String>();
            BufferedReader br = null;
            try {
                String line = null;
                br = new BufferedReader(new FileReader(new File(fileName)));
                while((line = br.readLine()) != null) {
                    uids.add(line);
                }

            } catch (FileNotFoundException e) {
                LOG.info("file: " + fileName + "does not exists... ");
            } catch (Exception e) {
                LOG.error("read from " + fileName + " get Exception... " + e);
            } finally {
                try {
                    if (br != null)
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return uids;
        }
    }


}
