/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.PrimitiveOperator;

@PrimitiveOperator(description = "Passes all input unchanged from its input port to its output port.")
@InputPortSet(cardinality = 1)
@OutputPortSet(cardinality = 1)
public class PassThrough extends AbstractOperator {

    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        StreamingOutput<OutputTuple> out = this.getOutput(0);
        out.submit(tuple);
    }
}
