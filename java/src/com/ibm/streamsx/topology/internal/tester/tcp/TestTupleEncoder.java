/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.tester.tcp;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

/**
 * Decode network bytes into a tuple for a specific output port using natural
 * Java mappings for all supported attributes. The schema of the output port
 * must match the incoming data, thus all tuple attributes must be present in
 * the raw bytes.
 * <P>
 * Handles a subset of the SPADE types, see the code for doDecode.
 * 
 */
public class TestTupleEncoder extends ProtocolEncoderAdapter {

    public TestTupleEncoder() {
    }

    @Override
    public void encode(IoSession session, Object message,
            ProtocolEncoderOutput out) throws Exception {

        TestTuple tuple = (TestTuple) message;

        IoBuffer buffer = IoBuffer
                .allocate(4 + 4 + tuple.getTupleData().length);
        buffer.putInt(tuple.getTesterId());
        buffer.putInt(tuple.getTupleData().length);
        buffer.put(tuple.getTupleData());
        buffer.flip();

        out.write(buffer);
    }
}
