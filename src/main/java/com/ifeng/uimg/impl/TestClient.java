package com.ifeng.uimg.impl;

import com.ifeng.uimg.Interface.UimgService;
import com.ifeng.uimg.Interface.Uimglabels;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

/**
 * 11/24/17
 */
public class TestClient {
    public static void main(String[] args) {
        Client client = new Client();
//        UimgService.Client cli = client.startClient("10.90.18.9",8765);
        UimgService.Client cli = client.startClient("127.0.0.1",8765);
        try {
            List<Uimglabels> recommendList = new ArrayList<Uimglabels>();
            Uimglabels uimglabels = new Uimglabels();
//            List<UserPage> userPageList = new ArrayList<UserPage>();

//            cli.getlabels();
//            cli.pushUserPages(userPageList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        client.close();
    }
}
