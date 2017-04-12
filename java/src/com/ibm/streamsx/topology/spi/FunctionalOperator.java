package com.ibm.streamsx.topology.spi;

import java.io.Closeable;

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
    void initialize() throws Exception;

    /**
     * Return the Streams Operator context.
     * 
     * This allows full access to the Streams runtime environment.
     * 
     * @return
     */
    OperatorContext getStreamsContext();

    /**
     * Get the functional context seen by the logic.
     * 
     * This is effectively a clean subset of OperatorContext that hides any
     * operator specific functionality.
     */
    FunctionContext getFunctionContext();
}
