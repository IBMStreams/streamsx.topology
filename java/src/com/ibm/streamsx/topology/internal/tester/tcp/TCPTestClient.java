/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.tester.tcp;

import java.net.InetSocketAddress;
import java.util.logging.Logger;

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.filterchain.IoFilter;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

public class TCPTestClient {

    private NioSocketConnector connector = new NioSocketConnector();
    private final InetSocketAddress addr;
    private IoSession session;
    
    private static final Logger TRACE = Logger.getLogger(TCPTestClient.class.getName());

    public TCPTestClient(InetSocketAddress addr) {
        this.addr = addr;
        connector.setConnectTimeoutMillis(5000);

        IoFilter tupleEncoder = new ProtocolCodecFilter(new TestTupleEncoder(),
                new TestTupleDecoder());

        connector.getFilterChain().addLast("tuples", tupleEncoder);

        connector.setHandler(new IoHandlerAdapter());
    }

    public synchronized void connect() throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            try {
                TRACE.info("Attempting to connect to test collector: " + addr);
                ConnectFuture future = connector.connect(addr);
                future.awaitUninterruptibly();
                session = future.getSession();
                TRACE.info("Connected to test collector: " + addr);
                return;
            } catch (RuntimeIoException e) {
                e.printStackTrace(System.err);
                if (i < 4) {
                    TRACE.warning("Failed to connect to test collector - retrying: " + addr);
                    Thread.sleep(1000);
                } else {
                    TRACE.severe("Failed to connect to test collector: " + addr);
                    throw e;
                }
            }
        }
        
    }

    public synchronized WriteFuture writeTuple(Object msg)
            throws InterruptedException {
        if (session == null)
            connect();
        return session.write(msg);
    }

    public synchronized void close() throws InterruptedException {
        session.close(false).await();
        connector.dispose();
    }

}
