package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

abstract class FunctionQueueableFunctor<T> extends FunctionFunctor implements StreamHandler<T> {
    
    private int queueSize;
    
    private SPLMapping<T> inputMapping;
    private StreamHandler<T> handler;
    
    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        // TODO Auto-generated method stub
        super.initialize(context);
        inputMapping = getInputMapping(this, 0);
        if (getQueueSize() == 0)
            handler = this;
        else
            handler = new FunctionalQueue<T>(context, getQueueSize(), this);
    }
    
    @Override
    public final void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        T value = inputMapping.convertFrom(tuple);
        handler.tuple(value);
    }
    
    @Override
    public final void processPunctuation(StreamingInput<Tuple> port, Punctuation mark)
            throws Exception {
        handler.mark(mark);
    }

    public int getQueueSize() {
        return queueSize;
    }

    @Parameter(optional=true)
    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }
}
