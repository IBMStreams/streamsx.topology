package com.ibm.streamsx.topology.spi.ops;

import java.io.Closeable;
import java.io.IOException;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;
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
@PrimitiveOperator
@InputPorts(@InputPortSet(cardinality = 1))
@SharedLoader
public class ForEach extends FunctionSink implements Closeable {
    
    /**
     * Operator initialization.
     * This implementation does nothing.
     */
    public void initialize() throws Exception {       
    }
    
    /**
     * Return the SPL Operator context.
     * 
     * This allows full access to the SPL environment.
     * @return
     */
    public final OperatorContext getSplOpContext() {
        return super.getOperatorContext();
    }
    
    /**
     * Get the functional context seen by the logic.
     * 
     * This is effectively a clean subset of OperatorContext
     * that hides any operator specific functionality.
     */
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
