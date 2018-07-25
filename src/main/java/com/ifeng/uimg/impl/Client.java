package com.ifeng.uimg.impl;

import com.ifeng.uimg.Interface.UimgService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


public class Client {
    private TTransport transport;
    private UimgService.Client client;
    public  UimgService.Client startClient(String host, int port) {
        try {
//            transport = new TZlibTransport(new TSocket(host, port));
            transport = new TSocket(host, port);
            TJSONProtocol protocol = new TJSONProtocol(transport);
//            TProtocol protocol = new TBinaryProtocol(transport);
            client = new UimgService.Client(protocol);
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

