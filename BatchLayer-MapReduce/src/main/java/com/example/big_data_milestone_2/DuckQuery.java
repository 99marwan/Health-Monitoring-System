package com.example.big_data_milestone_2;

import java.sql.*;
public class DuckQuery {
    public ResultSet getQuery(long start, long end) throws SQLException, ClassNotFoundException {
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT serviceName, avg(cpu), avg(ram), avg(disk), avg(maxCPU), " +
                "avg(maxRam), MAX(disk), sum(count)" +
                " FROM \"/home/hadoopuser/Downloads/parquet/*.parquet\"" +
                "where stamp BETWEEN " +  start +  " and " +  end +
                "GROUP BY serviceName");


        return rs;

    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {


    }
}
