package com.xingcloud.operations.utils;

import com.xingcloud.mysql.MySqlDict;

/**
 * Created by wanghaixing on 15-1-6.
 */
public class Constants {

    public static final MySqlDict dict = MySqlDict.getInstance();

    public static String HBASE_PORT = "3181";

    public static final String ATTRIBUTE_TABLE = "user_attribute";

    public static final String USER_COLUMNFAMILY = "v";

    public static final String USER_QUALIFIER = "v";

    public static long WRITE_BUFFER_SIZE = 1024 * 1024 * 20;

    public static boolean TableWalSwitch = false;

    public static String local_path_mysql_dump = "/data2/mysqldump/";
}
