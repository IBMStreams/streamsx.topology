/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.tester.tcp;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

/**
 * Decode network bytes into a tuple for a specific output port using natural
 * Java mappings for all supported attributes. The schema of the output port
 * must match the incoming data, thus all tuple attributes must be present in
 * the raw bytes.
 * <P>
 * Handles a subset of the SPADE types, see the code for doDecode.
 * 
 */
public class TestTupleDecoder extends CumulativeProtocolDecoder {

    public TestTupleDecoder() {
    }

    /**
     * Decode bytes an attribute at a time, once enough information exists to
     * maintain a tuple This code maintains state in the session as attributes,
     * namely:
     * <UL>
     * <LI>tuple - The partially initialized tuple to be sent to the next
     * handler in the chain.
     * <LI>attributeIndex - The next attribute to be decoded
     * </UL>
     */
    @Override
    protected boolean doDecode(IoSession session, IoBuffer in,
            ProtocolDecoderOutput out) throws Exception {

        Integer testerId = null;

        if (!session.containsAttribute("testerId")) {
            if (in.remaining() < 4)
                return false;

            testerId = in.getInt();
        }

        if (!in.prefixedDataAvailable(4)) {
            if (testerId != null)
                session.setAttribute("testerId", testerId);
            return false;
        }

        if (testerId == null) {
            testerId = (Integer) session.removeAttribute("testerId");
        }

        int tupleLength = in.getInt();

        byte[] tupleData = new byte[tupleLength];
        in.get(tupleData);

        out.write(new TestTuple(testerId, tupleData));

        return in.remaining() >= 4;
    }
}
