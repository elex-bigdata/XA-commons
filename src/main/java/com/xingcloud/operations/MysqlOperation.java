package com.xingcloud.operations;

import com.xingcloud.mysql.MySqlResourceManager;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.mysql.UserProp;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Author: liqiang
 * Date: 15-1-9
 * Time: 上午11:46
 */
public class MysqlOperation {

    private static final Log LOG = LogFactory.getLog(MysqlOperation.class);

    public static void main(String[] args) throws SQLException {
        MysqlOperation op = new MysqlOperation();
        op.clearOldData();
    }

    public void clearOldData() throws SQLException {
//        String[] pids = new String[]{"sof-wpm", "sof-zip", "sof-windowspm", "quick-start"};
//        String[] pids = new String[]{"sof-ient", "sof-newgdp", "sof-newgdppop", "sof-yacnvd"};
        String[] pids = new String[]{"i18n-status", "web337", "lignting-speeddial", "sof-dsk"};

        ExecutorService service = Executors.newFixedThreadPool(1);
        for(String pid : pids){
            service.submit(new MysqlExecutor(pid));
        }
        service.shutdown();
        LOG.info(" All finished " );
    }

    class MysqlExecutor implements Runnable{

        private String pid;

        public MysqlExecutor(String pid){
            this.pid = pid;
        }

        @Override
        public void run() {
            LOG.info(" begin delete " + pid );
            Connection conn = null;
            Statement statement = null;

            try {
                List<UserProp> userProps = MySqlResourceManager.getInstance().getUserPropsFromLocal(pid);
                List<Long> uids = getOldUids();

                List<String> sqls = new ArrayList<String>();
                StringBuilder uidSql = null;
                for(int i=0;i<uids.size();i++){
                    if(i % 10000 == 0){
                        if(uidSql != null){
                            sqls.add(uidSql.toString());
                        }
                        uidSql = new StringBuilder(String.valueOf(uids.get(i)));
                    }else {
                        uidSql.append(",").append(uids.get(i));
                    }
                }
                sqls.add(uidSql.toString());

                conn = MySql_16seqid.getInstance().getConnLocalNode(pid);
                statement = conn.createStatement();
                for(UserProp prop: userProps){
                    long begin = System.currentTimeMillis();
                    if("last_login_time".equals(prop.getPropName())){
                        continue;
                    }

                    int i = 0;
                    for(String batch : sqls) {
                        String sql = "delete from " + prop.getPropName() + " where uid in (" + batch + ")";
                        statement.execute(sql);
                        LOG.info("delete batch " + i + " " + prop.getPropName());
                        i++;
                    }
                    LOG.info(" delete " + pid + " " + prop.getPropName() + " finished cost " + (System.currentTimeMillis() - begin) + "ms");
                }

                int i = 0;
                for(String batch : sqls) {
                    String sql = "delete from last_login_time where uid in (" + batch + ")";
                    statement.execute(sql);
                    LOG.info("delete batch " + i + " last_login_time");
                    i++;
                }

            } catch (SQLException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            } finally {
                DbUtils.closeQuietly(statement);
                DbUtils.closeQuietly(conn);
            }
        }

        public List<Long> getOldUids(){
            List<Long> uids = new ArrayList<Long>();
            Connection conn = null;
            Statement statement = null;
            ResultSet rs = null;

            long begin = System.currentTimeMillis();
            try {
                LOG.info(" begin load uid of " + pid );
                conn = MySql_16seqid.getInstance().getConnLocalNode(pid);
                String uidSql = "select uid from last_login_time where val < 20140910000000";
                statement = conn.createStatement();
                rs = statement.executeQuery(uidSql);
                while(rs.next()){
                    uids.add(rs.getLong("uid"));
                }
                LOG.info(" load " + pid + " " + " uid finished, cost " + (System.currentTimeMillis() - begin) + "ms, size " + uids.size());
            } catch (SQLException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            } finally {
                DbUtils.closeQuietly(statement);
                DbUtils.closeQuietly(rs);
                DbUtils.closeQuietly(conn);
            }
            return uids;
        }
    }

}
