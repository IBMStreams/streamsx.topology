package com.ibm.streamsx.topology.spi.operators;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.internal.functional.ops.AbstractPipe;

/**
 * A functional pipe operator.
 * 
 * This is part of the SPI to allow additional functional style
 * functionality to built using the primitives provided by this toolkit.
 * 
 * Only methods explicitly declared in this class are part of the API.
 * Use of super-class methods (except those defined by {@code java.lang.Object)}
 * is not recommended and such methods may change or be removed at any time.
 */

@InputPorts(@InputPortSet(cardinality = 1))
@OutputPorts(@OutputPortSet(cardinality = 1))
@SharedLoader
public abstract class Pipe extends AbstractPipe implements FunctionalOperator {
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final OperatorContext getStreamsContext() {
        return super.getOperatorContext();
    }
    
    public Consumer<Object> getLogic() {
        return super.getLogic();
    }
    
    public void submit(Object value) {
        super.submit(value);
    }
    
    /**
     * Return a function that submits tuples to an output ports.
     */
    public final Consumer<Object> submitter() {
        return super.submitter();
    }
}
