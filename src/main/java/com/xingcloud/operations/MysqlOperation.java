package com.xingcloud.operations;

import com.xingcloud.mysql.MySqlResourceManager;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.mysql.UserProp;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
        String[] pids = new String[]{"sof-wpm", "sof-zip", "sof-windowspm", "quick-start"};

        ExecutorService service = Executors.newFixedThreadPool(2);
        for(String pid : pids){
            List<UserProp> userProps = MySqlResourceManager.getInstance().getUserPropsFromLocal(pid);
            for(UserProp prop: userProps){
                service.submit(new MysqlExecutor(pid, prop.getPropName()));
            }
        }
        service.shutdown();
    }

    class MysqlExecutor implements Runnable{

        private String pid;
        private String property;

        public MysqlExecutor(String pid, String property){
            this.pid = pid;
            this.property = property;
        }

        @Override
        public void run() {
            LOG.info(" begin delete " + pid + " " + property );
            Connection conn = null;
            Statement statement = null;
            String sql = "delete from " + property + " where uid in (select uid from last_login_time where val < 20140606000000)";

            long begin = System.currentTimeMillis();
            try {
                conn = MySql_16seqid.getInstance().getConnLocalNode(pid);
                statement = conn.createStatement();
                statement.execute(sql);
                LOG.info(" delete " + pid + " " + property + " finished cost " + (System.currentTimeMillis() - begin) + "ms");
            } catch (SQLException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            } finally {
                DbUtils.closeQuietly(statement);
                DbUtils.closeQuietly(conn);
            }
        }
    }

}
