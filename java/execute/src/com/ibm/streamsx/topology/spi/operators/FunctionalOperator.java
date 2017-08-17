package com.ibm.streamsx.topology.spi.operators;

import java.io.Closeable;
import java.io.IOException;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streamsx.topology.function.FunctionContext;

/**
 * A functional for each (sink) operator.
 * 
 * This is part of the SPI to allow additional functional style functionality to
 * built using the primitives provided by this toolkit.
 * 
 * Only methods explicitly declared in this class are part of the API. Use of
 * super-class methods (except those defined by {@code java.lang.Object)} is not
 * recommended and such methods may change or be removed at any time.
 */
public interface FunctionalOperator extends Closeable {

    /**
     * Operator initialization. This implementation does nothing.
     */
    default void initialize() throws Exception {
    }

    /**
     * Return the Streams Operator context.
     * 
     * This allows full access to the Streams runtime environment.
     * 
     * @return Streams operator context for this functional operator.
     */
    OperatorContext getStreamsContext();

    /**
     * Get the functional context seen by the logic.
     * 
     * This is effectively a clean subset of OperatorContext that hides any
     * operator specific functionality.
     */
    FunctionContext getFunctionContext();
    
    /**
     * Return the exception to be thrown on failure.
     * 
     * Allows a service provided to throw a specific exception
     * for a failure. Default implementation is to return {@code e}.
     * @param e Cause of failure.
     * @return Exception to be thrown.
     */
    default Throwable exception(Exception e) {
        return e;
    }
    
    /**
     * Operator shutdown.
     * This implementation does nothing.
     */
    @Override
    default void close() throws IOException {        
    }
}
