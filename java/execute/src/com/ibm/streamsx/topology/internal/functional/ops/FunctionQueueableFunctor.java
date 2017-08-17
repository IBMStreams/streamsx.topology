/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

/**
 * Queuing functional operator with a queue on port 0 if:
 * 
 * Parameter queueSize >= 0
 * AND
 * The input port is not connected to a PE port. In this case
 * there is already a thread for the processing.
 */
abstract class FunctionQueueableFunctor extends FunctionFunctor implements StreamHandler<Object> {
    
    private int queueSize;
    
    private SPLMapping<?> inputMapping;
    private StreamHandler<Object> handler;
    
    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);
        inputMapping = getInputMapping(this, 0);
        if (getQueueSize() <=0 || getInput(0).isConnectedToPEPort())
            handler = this; // not queued
        else
            handler = new FunctionalQueue<Object>(context, getQueueSize(), this);
    }
    
    @Override
    public final void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        Object value = inputMapping.convertFrom(tuple);
        handler.tuple(value);
    }
    
    @Override
    public final void processPunctuation(StreamingInput<Tuple> port, Punctuation mark)
            throws Exception {
        handler.mark(mark);
        super.processPunctuation(port, mark);
    }

    public int getQueueSize() {
        return queueSize;
    }

    @Parameter(optional=true)
    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }
}
