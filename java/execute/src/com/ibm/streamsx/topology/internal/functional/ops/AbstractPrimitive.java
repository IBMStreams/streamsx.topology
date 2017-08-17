package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;
import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getOutputMapping;
import static com.ibm.streamsx.topology.internal.functional.ops.FunctionalOpUtils.throwError;

import java.util.ArrayList;
import java.util.List;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.function.ObjIntConsumer;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;
import com.ibm.streamsx.topology.spi.operators.FunctionalOperator;

public abstract class AbstractPrimitive extends FunctionFunctor implements FunctionalOperator {
    
    private FunctionalHandler<ObjIntConsumer<Object>> processor;
    private List<SPLMapping<Object>> inputMappings;
    private List<SPLMapping<Object>> outputMappings;
    
    private String[] outputSerializer;
    private String[] inputSerializer;
    
    @Parameter(optional=true)
    public void setOutputSerializer(String[] outputSerializer) {
        this.outputSerializer = outputSerializer;
    }

    @Parameter(optional=true)
    public void setInputSerializer(String[] inputSerializer) {
        this.inputSerializer = inputSerializer;
    }

    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
              
        processor = createLogicHandler();
         
        inputMappings = new ArrayList<>(context.getNumberOfStreamingInputs());        
        for (int p = 0; p < context.getNumberOfStreamingInputs(); p++) {
            String serializer = null;
            if (inputSerializer != null)
                serializer = inputSerializer[p];
            inputMappings.add(getInputMapping(this, p, serializer));
        }
        
        outputMappings = new ArrayList<>(context.getNumberOfStreamingOutputs());
        for (int p = 0; p < context.getNumberOfStreamingOutputs(); p++) {
            String serializer = null;
            if (outputSerializer != null)
                serializer = outputSerializer[p];
            outputMappings.add(getOutputMapping(this, p, serializer));
        }
        
        try {
            initialize();
        } catch (Exception e) {
            throw throwError(exception(e));
        }
    }
    
    abstract public void initialize() throws Exception;
    
    /**
     * Get the functional context seen by the logic.
     * 
     * This is effectively a clean subset of OperatorContext
     * that hides any operator specific functionality.
     */
    @Override
    public final FunctionContext getFunctionContext() {
        return super.getFunctionContext();
    }

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
        final int port = stream.getPortNumber();
        
        try {

            final Object value = inputMappings.get(port).convertFrom(tuple);

            processor.getLogic().accept(value, port);
            
        } catch (Exception e) {
            throw throwError(exception(e));
        }
    }
    public ObjIntConsumer<Object> getLogic() {
        return processor.getLogic();
    }
    
    protected ObjIntConsumer<Object> submitter() {
        return (value, port) -> submit(value, port);
    }
    
    private void submit(final Object value, final int port) {
        final Tuple tuple = outputMappings.get(port).convertTo(value);
        
        try {
            getOutput(port).submit(tuple);
        } catch (Exception e) {
            throw new RuntimeException(throwError(exception(e)));
        }
    }
}
