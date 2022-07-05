package org.example;

import java.sql.*;
public class DuckQuery {
    void getQuery(int ID,long t1,long t2) throws SQLException, ClassNotFoundException {
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT serviceName,count(*),avg(cpu),avg(((ram_Total-ram_Free)/RAM_Total)*100),avg(((disk_Total-disk_Free)/disk_Total)*100) FROM \"/home/hadoopuser/IdeaProjects/speed4/parquet/par"+ID+"/*.parquet\" WHERE stamp BETWEEN "+ t1+" AND "+ t2 +" GROUP BY (serviceName) ");
        while (rs.next()) {
            System.out.print(rs.getString(1)+" / ");
            System.out.print(rs.getInt(2)+" / ");
            System.out.print(rs.getDouble(3)+" / ");
            System.out.print(rs.getDouble(4)+" / ");
            System.out.println(rs.getDouble(5)+" ");
        }

        System.out.println("First Query ~~~");
        ResultSet rs2 = stmt.executeQuery("SELECT serviceName,stamp, cpu from \"/home/hadoopuser/IdeaProjects/speed4/parquet/par"+ID+"/*.parquet\" JOIN  ( Select serviceName as serviceName2, max(cpu) as CPU2 From \"/home/hadoopuser/IdeaProjects/speed4/parquet/par"+ID+"/*.parquet\" WHERE stamp BETWEEN "+ t1+" AND "+ t2 +" Group By serviceName2) ON cpu = CPU2 AND serviceName=serviceName2 WHERE stamp BETWEEN "+ t1+" AND "+ t2);
        while (rs2.next()) {
            System.out.print(rs2.getString(1)+" / ");
            System.out.print(rs2.getLong(2)+" / ");
            System.out.println(rs2.getDouble(3)+" / ");
        }
        System.out.println("Second Query ~~~");
        ResultSet rs3 = stmt.executeQuery("SELECT serviceName,stamp, (((ram_Total-ram_Free)/ram_Total)*100)  From \"/home/hadoopuser/IdeaProjects/speed4/parquet/par"+ID+"/*.parquet\" JOIN  ( Select serviceName as serviceName2, max(((ram_Total-ram_Free)/ram_Total)*100) as RAM2 From \"/home/hadoopuser/IdeaProjects/speed4/parquet/par"+ID+"/*.parquet\" WHERE stamp BETWEEN "+ t1+" AND "+ t2 +" Group By serviceName2) ON (((ram_Total-ram_Free)/ram_Total)*100) = RAM2  AND serviceName=serviceName2 WHERE stamp BETWEEN "+ t1+" AND "+ t2);
        while (rs3.next()) {
            System.out.print(rs3.getString(1)+" / ");
            System.out.print(rs3.getLong(2)+" / ");
            System.out.println(rs3.getDouble(3)+" / ");
        }
        System.out.println("Third Query ~~~");
        ResultSet rs4 = stmt.executeQuery("SELECT serviceName,stamp, (((disk_Total-disk_Free)/disk_Total)*100) From \"/home/hadoopuser/IdeaProjects/speed4/parquet/par"+ID+"/*.parquet\" JOIN  ( Select serviceName as serviceName2, max(((disk_Total-disk_Free)/disk_Total)*100) as DISK2 From \"/home/hadoopuser/IdeaProjects/speed4/parquet/par"+ID+"/*.parquet\" WHERE stamp BETWEEN "+ t1+" AND "+ t2 +" Group By serviceName2) ON (((disk_Total-disk_Free)/disk_Total)*100) = DISK2 AND serviceName=serviceName2 WHERE stamp BETWEEN "+ t1+" AND "+ t2);
        while (rs4.next()) {
            System.out.print(rs4.getString(1)+" / ");
            System.out.print(rs4.getLong(2)+" / ");
            System.out.println(rs4.getDouble(3)+" / ");
        }
        System.out.println("END~~");
    }
}