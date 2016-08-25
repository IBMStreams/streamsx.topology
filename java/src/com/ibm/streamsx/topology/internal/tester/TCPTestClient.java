/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.tester;

import java.net.InetSocketAddress;

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

    public TCPTestClient(InetSocketAddress addr) {
        this.addr = addr;
        connector.setConnectTimeoutMillis(5000);

        IoFilter tupleEncoder = new ProtocolCodecFilter(new TestTupleEncoder(),
                new TestTupleDecoder());

        connector.getFilterChain().addLast("tuples", tupleEncoder);

        connector.setHandler(new IoHandlerAdapter());
    }

    public synchronized void connect() throws InterruptedException {
        for (;;) {
            try {
                ConnectFuture future = connector.connect(addr);
                future.awaitUninterruptibly();
                session = future.getSession();
                return;
            } catch (RuntimeIoException e) {
                System.err.println("Failed to connect.");
                e.printStackTrace();
                Thread.sleep(5000);
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
