/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.tester.tcp;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.mina.core.filterchain.IoFilter;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

/**
 * A TCP/IP listener source that uses Apache MINA as the server framework.
 * Clients send formatted data whose protocol is decoded into tuples and then
 * sent on the output port(s). MINA provides separation from the networking
 * layer, the protocol coding layer and the application layer.
 * <P>
 * Network handling by MINA involves a chain of filters followed by a I/O
 * handler. The filters processing the incoming raw bytes into a logical
 * application object that will be handled by application specific code that
 * implements IOHandler.
 * <P>
 * A ProtocolCodecFilter is used to convert the in-coming bytes into a tuple.
 * ProtocolCodecFilter simplifies the handling of items that are split across
 * multiple network packets (which may occur in TCP/IP regardless of how the
 * data was sent, that is a single write by the client may result in multiple
 * reads by the server). <BR>
 * 
 */
public class TCPTestServer {

    private final IoAcceptor acceptor;
    private InetSocketAddress bindAddress;

    /**
     * Initialize the MINA server.
     */
    public TCPTestServer(int port, boolean loopback, IoHandler handler) throws Exception {

        acceptor = new NioSocketAcceptor();

        IoFilter tupleEncoder = new ProtocolCodecFilter(new TestTupleEncoder(),
                new TestTupleDecoder());

        acceptor.getFilterChain().addLast("testtuples", tupleEncoder);

        acceptor.setHandler(handler);

        // Get the bind address now so the majority of
        // errors are caught at initialization time.
        bindAddress = new InetSocketAddress(
                loopback ? InetAddress.getLoopbackAddress() : InetAddress.getLocalHost(), port);
    }

    public InetSocketAddress start() throws Exception {
        acceptor.bind(bindAddress);
        return (InetSocketAddress) acceptor.getLocalAddress();
    }

    public void shutdown() throws Exception {
        acceptor.unbind();
    }
}
