/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;
import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getLogicObject;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

/**
 * Generically adds an int32 hash value as the second
 * attribute to a stream.
 */
@PrimitiveOperator
@Icons(location16 = "opt/icons/functor_16.gif", location32 = "opt/icons/functor_32.gif")
@InputPorts({@InputPortSet(cardinality = 1)})
@OutputPorts({@OutputPortSet(cardinality = 1)})
public class HashAdder extends FunctionFunctor {
    
    private ToIntFunction<Object> hasher;

    protected SPLMapping<Object> mapping;
    protected StreamingOutput<OutputTuple> output;

    @Override
    public void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);
        
        hasher = getLogicObject(getFunctionalLogic());

        output = getOutput(0);
        mapping = getInputMapping(this, 0);
    }

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        // Take the hash code, add it to the tuple, and submit.
        Object value = mapping.convertFrom(tuple);
        OutputTuple ot = output.newTuple();
        ot.setObject(0, tuple.getObject(0));
        ot.setInt(1, hasher.applyAsInt(value));
        output.submit(ot);
    }
}
