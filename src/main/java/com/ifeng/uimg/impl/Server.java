package com.ifeng.uimg.impl;

import com.ifeng.uimg.Interface.UimgService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.protocol.TJSONProtocol;
/**
 * 2018.07.20
 */
public class Server {

    private static Log LOGGER = LogFactory.getLog(Server.class);

    public void startServer() {
        try {

            TServerSocket serverTransport = new TServerSocket(8765);
//            TServerSocket serverTransport = new TServerSocket(8765);
            UimgService.Processor process = new UimgService.Processor(new UimgImpl());
            Factory portFactory = new TBinaryProtocol.Factory(true, true);

            Args args = new Args(serverTransport);
            args.processor(process);
            args.protocolFactory(portFactory);

            TServer server = new TThreadPoolServer(args);
            server.serve();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Server server = new Server();
        server.startServer();
    }
}
