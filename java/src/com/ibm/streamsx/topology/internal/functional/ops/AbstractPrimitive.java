package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;
import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getOutputMapping;

import java.util.ArrayList;
import java.util.List;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.function.ObjIntConsumer;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

public abstract class AbstractPrimitive extends FunctionFunctor {
    
    private FunctionalHandler<ObjIntConsumer<Object>> processor;
    private List<SPLMapping<Object>> inputMappings;
    private List<SPLMapping<Object>> outputMappings;
    
    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
              
        processor = createLogicHandler();
        
        initialize();
        
        inputMappings = new ArrayList<>(context.getNumberOfStreamingInputs());        
        for (int p = 0; p < context.getNumberOfStreamingInputs(); p++)
            inputMappings.add(getInputMapping(this, p));
        
        outputMappings = new ArrayList<>(context.getNumberOfStreamingOutputs());
        for (int p = 0; p < context.getNumberOfStreamingOutputs(); p++)
            outputMappings.add(getOutputMapping(this, p));
        
    }
    
    abstract public void initialize() throws Exception;

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
        final int port = stream.getPortNumber();
        
        final Object value = inputMappings.get(port).convertFrom(tuple);
        
        processor.getLogic().accept(value, port);
    }
    
    protected ObjIntConsumer<Object> submitter() {
        return (value, port) -> submit(value, port);
    }
    
    private void submit(final Object value, final int port) {
        final Tuple tuple = outputMappings.get(port).convertTo(value);
        
        try {
            getOutput(port).submit(tuple);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
