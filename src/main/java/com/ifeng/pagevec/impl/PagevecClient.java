package com.ifeng.pagevec.impl;

import com.ifeng.pagevec.inter.PagevecService;
import java.util.List;

/**
 * 2018.07.20
 */
public class PagevecClient {

    private static final String INSERT_INTO_APP_PAGE = "insert into dmp.app_pages " +
            "(pid,title,vec) " +
            "VALUES (?,?,?);";

    public void startClient() {

        PagevecCassan pCassan = new PagevecCassan();
        String[] nodes = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155", "10.80.25.155"};
        int port = 9042;
        pCassan.connect(nodes, port);
        String query = "select * from dmp.app_pages";
        pCassan.loadData(query,INSERT_INTO_APP_PAGE);
        pCassan.close();

    }

    public static void main(String[] args) {
        PagevecClient pCli = new PagevecClient();
        pCli.startClient();
/*        Client client = new Client();
        PagevecService.Client cli = client.startClient("10.80.93.152",9090);
        try {
            String str = "如果";
            List<Double> vector = cli.getlabels(str);
            System.out.println("size: " + vector.size());
            for(double d: vector) {
                System.out.print(d + ",");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }*/



/*        Client client = new Client();
        PagevecService.Client cli = client.startClient("10.80.93.152",9090);
        try {
            String[] str = {"null","","!","@#1","sert","向量","，"};
            for(String s : str) {
            List<Double> vector = cli.getlabels(s);
            System.out.println("size: " + vector.size());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }*/
    }
}
