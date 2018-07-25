package com.ifeng.pagevec.impl;

import com.ifeng.pagevec.inter.PagevecService;
import com.ifeng.pagevec.impl.Client;

import java.util.ArrayList;
import java.util.List;

/**
 * 2018.07.20
 */
public class TestClient {
    public static void main(String[] args) {
        Client client = new Client();
        PagevecService.Client cli = client.startClient("10.80.93.152",9090);
        try {
            String str = "西安市公安局人才落户将上门服务";
            List<Double> vector = new ArrayList<Double>(cli.getlabels(str));
            for(double d: vector) {
                System.out.print(d + ",");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        client.close();
    }
}
