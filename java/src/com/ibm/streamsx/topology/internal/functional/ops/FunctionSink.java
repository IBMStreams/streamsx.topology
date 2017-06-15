/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

public abstract class FunctionSink extends FunctionFunctor {
    private FunctionalHandler<Consumer<Object>> sinkerHandler;
    private SPLMapping<?> mapping;
    private String tupleSerializer;

    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);

        sinkerHandler = createLogicHandler();
        
        mapping = getInputMapping(this, 0);
        
        initialize();
    }
    
    @Parameter(optional=true)
    public final void setTupleSerializer(String tupleSerializer) {
        this.tupleSerializer = tupleSerializer;
    }
    
    protected void initialize() throws Exception {        
    }

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        Object value = mapping.convertFrom(tuple);
        final Consumer<Object> sinker = sinkerHandler.getLogic();
        synchronized (sinker) {
            sinker.accept(value);
        }
    }
}
