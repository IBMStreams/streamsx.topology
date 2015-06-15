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
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.topology.function7.BiFunction;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

@PrimitiveOperator
@InputPortSet(cardinality = 1)
@OutputPortSet(cardinality = 1)
@Icons(location16 = "opt/icons/functor_16.gif", location32 = "opt/icons/functor_32.gif")
public class FunctionConvertToSPL<T> extends FunctionFunctor {

    private BiFunction<T, OutputTuple, OutputTuple> convert;
    private SPLMapping<T> inputMapping;
    private StreamingOutput<OutputTuple> output;

    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);

        convert = getLogicObject(getFunctionalLogic());
        output = getOutput(0);
        inputMapping = getInputMapping(this, 0);
    }

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        T value = inputMapping.convertFrom(tuple);
        OutputTuple outTuple = output.newTuple();
        synchronized (convert) {
            outTuple = convert.apply(value, outTuple);
        }
        if (outTuple != null)
            output.submit(outTuple);
    }
}
