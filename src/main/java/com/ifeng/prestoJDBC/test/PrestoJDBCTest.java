package com.ifeng.prestoJDBC.test;

//import com.datastax.driver.core.*;
import com.ifeng.pagevec.impl.Client;
import com.ifeng.pagevec.inter.PagevecService;
import org.apache.thrift.TException;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * Presto JDBC 连接 Cassandra
 * 插入list格式的数据未实现
 */
public class PrestoJDBCTest {

    private static final String INSERT_INTO_APP_PAGE = "insert into dmp.app_pages " +
            "(pid,title,vec) " +
            "VALUES (?,?,?);";

    private void loadMySQL(String query, String insert) {
        Client client = new Client();
        PagevecService.Client cli = client.startClient("10.80.93.152", 9090);

        int counter = 0;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        String url = "jdbc:presto://10.80.75.142:8089/cassandra/dmp";
        Properties properties = new Properties();
        properties.setProperty("user", "");
        properties.setProperty("password", "");
        try {
            Connection conn = DriverManager.getConnection(url, properties);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                String pid = rs.getString("pid");
                String title = String.valueOf(rs.getString("title"));
                List<Double> vec = cli.getlabels(title);

                PreparedStatement ps = conn.prepareStatement(insert);
                ps.setString(1, pid);
                ps.setString(2, title);
                ps.setObject(3, vec);

                if (counter % 50000 == 0) {
                    System.out.println("insert vec into dmp.app_pages: " + counter +
                            " Time: " + dateFormat.format(new Date()));
                }
                counter++;
            }
            rs.close();
            conn.close();

            System.out.println("insert vec into dmp.app_pages: " + counter +
                    " Time: " + dateFormat.format(new Date()));
        } catch (SQLException e) {
            System.out.println("Presto error");
            e.printStackTrace();
        } catch (TException te) {
            System.out.println("Thrift error");
            te.printStackTrace();
        }


    }


    public static void main(String[] args) {

        PrestoJDBCTest client = new PrestoJDBCTest();
        String query = "select pid,title,vec from dmp.app_pages where vec is null";
        client.loadMySQL(query, INSERT_INTO_APP_PAGE);
    }
}
