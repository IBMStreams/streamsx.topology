package com.ibm.streamsx.topology.internal.functional.ops;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.function.ObjIntConsumer;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;

public abstract class AbstractPrimitive extends FunctionFunctor {
    
    private FunctionalHandler<ObjIntConsumer<Object>> processor;
    
    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
              
        processor = createLogicHandler();
        
        initialize();
    }
    
    abstract public void initialize() throws Exception;

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
        
        processor.getLogic().accept(tuple, stream.getPortNumber());
    }
}
