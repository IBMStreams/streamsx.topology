package com.ibm.streamsx.topology.spi.operators;

import java.io.IOException;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionSink;

/**
 * A functional for each (sink) operator.
 * 
 * This is part of the SPI to allow additional functional style
 * functionality to built using the primitives provided by this toolkit.
 * 
 * Only methods explicitly declared in this class are part of the API.
 * Use of super-class methods (except those defined by {@code java.lang.Object)}
 * is not recommended and such methods may change or be removed at any time.
 */
@InputPorts(@InputPortSet(cardinality = 1))
@SharedLoader
public abstract class ForEach extends FunctionSink implements FunctionalOperator {
    
    /**
     * Operator initialization.
     * This implementation does nothing.
     */
    @Override
    public void initialize() throws Exception {       
    }
    
    /**
     * Return the SPL Operator context.
     * 
     * This allows full access to the SPL environment.
     * @return
     */
    @Override
    public final OperatorContext getStreamsContext() {
        return super.getOperatorContext();
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

    /**
     * Operator shutdown.
     * This implementation does nothing.
     */
    @Override
    public void close() throws IOException {        
    }
}
