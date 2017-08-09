/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.tester.ops;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.mina.core.future.WriteFuture;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.encoding.BinaryEncoding;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.samples.patterns.TupleConsumer;
import com.ibm.streamsx.topology.internal.tester.tcp.TCPTestClient;
import com.ibm.streamsx.topology.internal.tester.tcp.TestTuple;

@PrimitiveOperator
@InputPortSet
@Libraries("opt/apache-mina-2.0.2/dist/*")
public class TesterSink extends TupleConsumer {
    
    public static final String KIND = "com.ibm.streamsx.topology.testing::TesterSink";

    private String host;
    private int port;
    private BinaryEncoding[] encoders;
    private TCPTestClient[] clients;

    @Override
    public void initialize(OperatorContext context) throws Exception {
        super.initialize(context);

        setBatchSize(1);
        setPreserveOrder(true);

        InetSocketAddress addr = new InetSocketAddress(getHost(), getPort());
        clients = new TCPTestClient[context.getNumberOfStreamingInputs()];
        encoders = new BinaryEncoding[context.getNumberOfStreamingInputs()];
        for (StreamingInput<Tuple> input : context.getStreamingInputs()) {
            TCPTestClient client = new TCPTestClient(addr);
            client.connect();
            clients[input.getPortNumber()] = client;

            encoders[input.getPortNumber()] = input.getStreamSchema()
                    .newNativeBinaryEncoding();
        }
    }

    @Override
    protected boolean processBatch(Queue<BatchedTuple> batch) throws Exception {
        List<WriteFuture> futures = new ArrayList<>(batch.size());
        for (BatchedTuple bt : batch) {
            int portIndex = bt.getStream().getPortNumber();
            TCPTestClient client = clients[portIndex];

            BinaryEncoding be = encoders[portIndex];
            byte[] tupleData = new byte[(int) be.getEncodedSize(bt.getTuple())];
            be.encodeTuple(bt.getTuple(), ByteBuffer.wrap(tupleData));
            TestTuple tt = new TestTuple(portIndex, tupleData);
            futures.add(client.writeTuple(tt));
        }
        for (WriteFuture future : futures) {
            future.await();
        }
        return false;
    }

    @Override
    public void processPunctuation(StreamingInput<Tuple> port, Punctuation mark)
            throws Exception {
        super.processPunctuation(port, mark);
        if (mark == Punctuation.FINAL_MARKER) {
            int portIndex = port.getPortNumber();
            TestTuple finalTupleMarker = new TestTuple(portIndex, new byte[0]);
            TCPTestClient client = clients[portIndex];
            client.writeTuple(finalTupleMarker).await();
        }
    }

    public int getPort() {
        return port;
    }

    @Parameter
    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    @Parameter
    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public void shutdown() throws Exception {
        for (TCPTestClient client : clients) {
            client.close();
        }
        super.shutdown();
    }

}
