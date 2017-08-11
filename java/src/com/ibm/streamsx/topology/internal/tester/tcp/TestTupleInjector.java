/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.tester.tcp;

import java.nio.ByteBuffer;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.encoding.BinaryEncoding;

/**
 * Injects tuples from the test-side TCP server into the test-side Java testable
 * graph where they will get sent to the handlers.
 * 
 */
class TestTupleInjector implements StreamHandler<byte[]> {

    private StreamingOutput<OutputTuple> injectPort;
    private final BinaryEncoding encoding;

    TestTupleInjector(StreamingOutput<OutputTuple> injectPort) {
        this.injectPort = injectPort;
        encoding = injectPort.getStreamSchema().newNativeBinaryEncoding();
    }

    @Override
    public void tuple(byte[] tupleData) throws Exception {
        if (tupleData.length == 0) {
            mark(Punctuation.FINAL_MARKER);
            return;
        }
        Tuple tuple = encoding.decodeTuple(ByteBuffer.wrap(tupleData));
        injectPort.submit(tuple);
    }

    @Override
    public void mark(Punctuation mark) throws Exception {
        injectPort.punctuate(mark);
    }
}
