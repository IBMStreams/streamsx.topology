package com.ibm.streamsx.topology.spi.operators;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionSource;

/**
 * A functional source operator.
 * 
 * This is part of the SPI to allow additional functional style
 * functionality to built using the primitives provided by this toolkit.
 * 
 * Only methods explicitly declared in this class are part of the API.
 * Use of super-class methods (except those defined by {@code java.lang.Object)}
 * is not recommended and such methods may change or be removed at any time.
 */
@OutputPorts(@OutputPortSet(cardinality = 1))
@SharedLoader
public abstract class Source extends FunctionSource implements FunctionalOperator {
    
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
     * Return the equivalent logic that was passed into
     * {@link com.ibm.streamsx.topology.spi.builder.Invoker#invokeSource(com.ibm.streamsx.topology.Topology, Class, Supplier, java.util.Map)}.
     * Note it will not be the same reference as the logic will have been serialized and de-serialized
     * for execution within the Streams context.
     */
    public final Supplier<Iterable<Object>> getLogic() {
        return super.getLogic();
    }
}
