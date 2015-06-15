/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

@PrimitiveOperator
@InputPortSet(cardinality = 1)
@OutputPortSet(cardinality = 1)
@Icons(location16 = "opt/icons/functor_16.gif", location32 = "opt/icons/functor_32.gif")
public class ObjectHashAdder<T> extends
        FunctionFunctor {

    private SPLMapping<T> mapping;
    private StreamingOutput<OutputTuple> output;

    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);

        output = getOutput(0);
        mapping = getInputMapping(this, 0);
    }

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        // Take the hash code, add it to the tuple, and submit.
        T value = mapping.convertFrom(tuple);
        OutputTuple ot = output.newTuple();
        ot.setObject(0, tuple.getObject(0));
        ot.setInt(1, value.hashCode());
        output.submit(ot);
    }

    /**
     * Removed this as a parameter.
     */
    @Override
    public void setFunctionalLogic(String logic) {
    }
}
