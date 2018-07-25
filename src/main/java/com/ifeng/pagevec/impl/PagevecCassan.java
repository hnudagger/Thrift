package com.ifeng.pagevec.impl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.ifeng.pagevec.inter.PagevecService;
import org.apache.thrift.TException;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PagevecCassan {

    Session session;
    Cluster cluster;

    private Session getSession() {
        return session;
    }

    public void connect(String[] node, int port) {
        SocketOptions so = new SocketOptions().setReadTimeoutMillis(30000).setConnectTimeoutMillis(30000);

        PoolingOptions poolingOptions = new PoolingOptions()
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 64)
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 2)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 6);
        QueryOptions queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
        RetryPolicy retryPolicy = DowngradingConsistencyRetryPolicy.INSTANCE;

        cluster = Cluster.builder()
                .addContactPoints(node)
                .withSocketOptions(so)
                .withPoolingOptions(poolingOptions)
                .withQueryOptions(queryOptions)
                .withRetryPolicy(retryPolicy)
                .withPort(port)
                .build();

        cluster.getConfiguration().getQueryOptions().setFetchSize(50);
        this.session = cluster.connect("dmp");
    }

    public void loadData(String query, String insert) {

        Client client = new Client();
        PagevecService.Client cli = client.startClient("10.80.93.152", 9090);

        ResultSet resultSet = getSession().execute(query);
        PreparedStatement prepareStatement = session.prepare(insert);

        int counter = 0;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        try {

            for (Row row : resultSet) {
                String pid = row.getString("pid");
                String title = String.valueOf(row.getString("title"));
                List<Double> vector = row.getList("vec", Double.class);

                if (vector.size() < 5) {
                    List<Double> vec = cli.getlabels(title);
                    BoundStatement bindStatement = new BoundStatement(prepareStatement)
                            .bind(pid, title, vec);
                    session.execute(bindStatement);
                }
                if (counter % 50000 == 0) {
                    System.out.println("insert vec into dmp.app_pages: " + counter +
                            " Time: " + dateFormat.format(new Date()));
                }
                counter++;
            }
            System.out.println("insert vec into dmp.app_pages: " + counter +
                    " Time: " + dateFormat.format(new Date()));

        } catch (OperationTimedOutException e) {
            System.out.println("TimeOutException: " + e.getMessage() +
                    " " + dateFormat.format(new Date()));
        } catch (TException tException) {
            System.out.println("TException: " + tException.getMessage() +
                    " " + dateFormat.format(new Date()));
        }

        client.close();
    }

    public void close() {
        session.close();
        cluster.close();
    }

}
