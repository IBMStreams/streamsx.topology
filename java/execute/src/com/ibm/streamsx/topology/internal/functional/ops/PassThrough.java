/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;

@InputPorts(@InputPortSet(cardinality = 1))
@OutputPorts(@OutputPortSet(cardinality = 1))
public abstract class PassThrough extends AbstractOperator {
    
    private StreamingOutput<OutputTuple> out;
    
    @ContextCheck(runtime=false)
    public static void checkNotConsistentRegionSource(OperatorContextChecker checker) {
        FunctionalOpUtils.checkNotConsistentRegionSource(checker);
    }
    
    @Override
    public void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
        out = getOutput(0);
    }

    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        out.submit(tuple);
    }
}
