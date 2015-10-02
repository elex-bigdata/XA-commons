package com.xingcloud.operations;

import com.xingcloud.id.s.MySQLResourceManager;
import com.xingcloud.operations.utils.Constants;
import com.xingcloud.operations.utils.DateManager;
import com.xingcloud.uidtransform.HbaseMysqlUIDTruncator;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
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
    public static String FIX_PATH = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/deleted_idmap/";

    private String date;
    public static int THREAD_NUM = 10;

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
//        String[] pids = new String[]{"sof-wpm", "sof-zip", "sof-windowspm", "quick-start","sof-ient", "sof-newgdp", "sof-newgdppop", "sof-yacnvd",
//                "i18n-status", "web337", "lightning-speedial", "sof-dsk", "sof-installer", "omiga-plus", "webssearches", "sweet-page", "infospace", "delta-homes"};

        String[] pids = new String[]{"22apple","22find","ttsgames","aartemis","awesomehp","cok337","delta-homes","sof-dp","sof-dsk","dosearches","sof-hpprotect","sof-everything","fishao",
                "gdp","sof-gdp","sof-seed","sof-ss","v9-gp","hot-finder","ie-lightning-speed","sof-ient","sof-isafe","isearch123","istart123","istartpageing","istartsurf",
                "key-find","lightning-newtab","lightning-speedial","lightning-speed-dial","luckybeginning","luckysearches","sof-macinstaller","myoivu",
                "mystartsearch","myv9","nationzoom","newgag","newtab2","internet-3","ordt","omiga-plus","omniboxes","oursurfing","sof-picexa-dl",
                "sof-px","portaldosites","qone8","qone8search","qvo6","raydownload","safehomepage","sof-pbd-dl","sof-wzp-dl","sof-yacbndl","sof-zbd-dl","sweet-page","v9",
                "v9m","v9search","vi-view","wartune-en","web337","webssearches","sof-zip","sof-wzpdl","sof-wpm","sof-wxz","www-337-com","lightningnewtab",
                "xa-xbb","yac-newdl","sof-yacnvd","yoursearching","websupport","shenqu","maomaomei","kjsg","xzqz","livepoolpro","desertoperations","wargame1942","generalsofwar",
                "monkeyking","darkorbit","loa","myfreezoo","mlf","farmerama","drakensang","piratestorm","guardiaoonline","dragon-pals","hog","cuponkit","cuponkit-ext","unnamedsoft",
                "unsoftnvd","chhp-unistallmaster","chhp-myoivu","prote-ff-extension","sof-installer","sof-newgdppop","qtype","qtyper","quick-sidebar","quick-start","searchprotect",
                "usv9","jiggybonga","xlfc","xlfc-cbnc","yzzt","csbhtw","kszl","ddt","gcld","gcld","gs","age","age2","agei","agei2","aoerts","ram","ba2","cok","cokfb","happyfarm",
                "coktw","cokmi","thor","rafo","firefox-searchengine","gggggg","do-search","wuzijing","unextnvd"};

//        THREAD_NUM = pids.length;
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
        private String date;
        private String fileName;

        public IdMapExecutor(String pid, String date) {
            this.pid = pid;
            this.date = date;
//            this.fileName = Constants.deleted_uids_path + pid + "/" + date + ".txt";
        }

        @Override
        public void run() {
            LOG.info(" begin delete " + pid );
            long begin = System.currentTimeMillis();
            Connection conn = null;
            Statement statement = null;
            List<String> uids = getDeletedUids(pid, date);
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
                    String sql = "delete from `vf_" + pid + "`.`id_map` where id in (" + batch + ")";
                    statement.execute(sql);
                    LOG.info("delete <" + pid + "> batch " + i + " ...");
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

        public List<String> getDeletedUids(String pid, String date) {
            List<String> uids = new ArrayList<String>();
            Configuration conf = new Configuration();
            InputStream in = null;
            BufferedReader br = null;
            try {
                String pidPath = FIX_PATH + pid + "/" + date;
                FileSystem fs = FileSystem.get(URI.create(FIX_PATH), conf);
                for(FileStatus fileStatus: fs.listStatus(new Path(pidPath))){
                    if(fileStatus.isFile()) {
                        Path path = fileStatus.getPath();
                        in = fs.open(path);
                        br = new BufferedReader(new InputStreamReader(in));
                        String line = null;
                        while((line = br.readLine()) != null) {
                            uids.add(line);
                        }
                    } else {
                        break;
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (br != null)
                        br.close();
                    if(in != null)
                        in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return uids;
        }

        /*public List<String> readFromFile(String fileName) {
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
        }*/
    }


}
