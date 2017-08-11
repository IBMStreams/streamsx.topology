package com.ibm.streamsx.topology.spi.operators;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.topology.function.ObjIntConsumer;
import com.ibm.streamsx.topology.internal.functional.ops.AbstractPrimitive;

/**
 * A functional operator.
 * 
 * This is part of the SPI to allow additional functional style
 * functionality to built using the primitives provided by this toolkit.
 * 
 * Only methods explicitly declared in this class are part of the API.
 * Use of super-class methods (except those defined by {@code java.lang.Object)}
 * is not recommended and such methods may change or be removed at any time.
 */
@SharedLoader
public abstract class Primitive extends AbstractPrimitive implements FunctionalOperator {
    
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
    
    public ObjIntConsumer<Object> getLogic() {
        return super.getLogic();
    }
    
    /**
     * Return a function that submits tuples to an output ports.
     */
    public final ObjIntConsumer<Object> submitter() {
        return super.submitter();
    }
}
