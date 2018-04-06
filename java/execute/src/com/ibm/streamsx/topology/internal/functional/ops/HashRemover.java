/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;

@InputPorts(@InputPortSet(cardinality = 1))
@OutputPorts(@OutputPortSet(cardinality = 1))
public abstract class HashRemover extends FunctionFunctor {

    private StreamingOutput<OutputTuple> output;

    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);
        output = getOutput(0);
    }

    /**
     * Removes the __spl_hash attribute at the start of a parallel region.
     */
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        OutputTuple out_t = output.newTuple();
        out_t.setObject(0, tuple.getObject(0));
        output.submit(out_t);
    }
    
    /**
     * Removed this as a parameter.
     */
    @Override
    public void setFunctionalLogic(String logic) {
    }
}
