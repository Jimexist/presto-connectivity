package com.madadata.eval.prestoconnectivity;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by jiayu on 9/9/16.
 */
public class HiveJdbcClient {

    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static final String hostName = "ec2-54-223-54-76.cn-north-1.compute.amazonaws.com.cn";
    private static final String tableName = "dummyTable";

    /**
     * @throws SQLException
     */
    public static void main(String[] args) throws Exception {

        Class.forName(driverName);

        Connection con = DriverManager.getConnection(String.format("jdbc:hive2://%s:10000/default", hostName), "hive", "");
        Statement stmt = con.createStatement();

        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create external table "
            + tableName
            + " (key int, value string) row format delimited fields terminated by ',' location 's3://luigi/staging/fake-account/dummy_table'");

        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }

        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
        }
    }
}