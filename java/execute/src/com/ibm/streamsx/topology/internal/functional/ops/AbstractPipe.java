package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;
import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getOutputMapping;
import static com.ibm.streamsx.topology.internal.functional.ops.FunctionalOpUtils.throwError;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;
import com.ibm.streamsx.topology.spi.operators.FunctionalOperator;

public abstract class AbstractPipe extends FunctionFunctor implements FunctionalOperator {
    
    private FunctionalHandler<Consumer<Object>> processor;
    private SPLMapping<Object> inputMapping;
    private SPLMapping<Object> outputMapping;
    
    private String outputSerializer;
    private String inputSerializer;
    
    private StreamingOutput<OutputTuple> outputPort;
    
    @Parameter(optional=true)
    public void setOutputSerializer(String outputSerializer) {
        this.outputSerializer = outputSerializer;
    }

    @Parameter(optional=true)
    public void setInputSerializer(String inputSerializer) {
        this.inputSerializer = inputSerializer;
    }

    @Override
    public final synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
        
        outputPort = getOutput(0);
              
        processor = createLogicHandler();
         
        inputMapping = getInputMapping(this, 0, inputSerializer);      
        outputMapping = getOutputMapping(this, 0, outputSerializer);
        
        try {
            initialize();
        } catch (Exception e) {
            throw throwError(exception(e));
        }
    }
    
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
    public final void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
        
        try {

            final Object value = inputMapping.convertFrom(tuple);

            processor.getLogic().accept(value);
            
        } catch (Exception e) {
            throw throwError(exception(e));
        }
    }
    
    protected Consumer<Object> getLogic() {
        return processor.getLogic();
    }
    
    protected Consumer<Object> submitter() {
        return value -> submit(value);
    }
    
    protected void submit(final Object value) {
        final Tuple tuple = outputMapping.convertTo(value);
        
        try {
            outputPort.submit(tuple);
        } catch (Exception e) {
            throw new RuntimeException(throwError(exception(e)));
        }
    }
}
