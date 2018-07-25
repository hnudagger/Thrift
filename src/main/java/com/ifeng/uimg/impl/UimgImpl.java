package com.ifeng.uimg.impl;

import com.datastax.driver.core.*;
import com.ifeng.uimg.Interface.*;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;


/**
 *
 */

public class UimgImpl implements UimgService.Iface {

    String[] CONTACT_POINTS = {"10.80.17.155"};
    int PORT = 9042;

    public Uimglabels getlabels(String uid) {

        return null;
    }

    public Map<String, Uimglabels> getulabels(List<String> uids) throws TException {

        Session session;
        Cluster cluster;
        cluster = Cluster.builder()
                .addContactPoints(CONTACT_POINTS).withPort(PORT)
                .build();
        session = cluster.connect("uimg");

        session.execute(
                "CREATE TABLE IF NOT EXISTS uimge_userpages (" +
                        "userid text PRIMARY KEY," +
                        "pagesid list<text>" +
                        ");"
        );

        session.close();
        cluster.close();

        return null;

    }
}


