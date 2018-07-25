package com.ifeng.pagevec.impl;

import com.ifeng.pagevec.inter.PagevecService;
import com.ifeng.uimg.Interface.UimgService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


public class Client {
    private TTransport transport;
    private PagevecService.Client client;
    public  PagevecService.Client startClient(String host, int port) {
        try {
//            transport = new TZlibTransport(new TSocket(host, port));
            transport = new TSocket(host, port);
            TJSONProtocol protocol = new TJSONProtocol(transport);
//            TProtocol protocol = new TBinaryProtocol(transport);
            client = new PagevecService.Client(protocol);
            transport.open();
            return client;
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void close() {
        transport.close();
    }

}

