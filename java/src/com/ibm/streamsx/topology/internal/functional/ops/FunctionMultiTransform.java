/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;
import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getLogicObject;
import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getOutputMapping;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

@PrimitiveOperator
@InputPortSet(cardinality = 1)
@OutputPortSet(cardinality = 1)
@Icons(location16 = "opt/icons/functor_16.gif", location32 = "opt/icons/functor_32.gif")
public class FunctionMultiTransform<T, U> extends FunctionFunctor {

    private Function<T, Iterable<U>> transform;
    private SPLMapping<T> inputMapping;
    private SPLMapping<U> outputMapping;
    private StreamingOutput<OutputTuple> output;

    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);

        setLogic(transform = getLogicObject(getFunctionalLogic()));
        output = getOutput(0);
        inputMapping = getInputMapping(this, 0);
        outputMapping = getOutputMapping(this, 0);
    }

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        T value = inputMapping.convertFrom(tuple);
        Iterable<U> modValues;
        synchronized (transform) {
            modValues = transform.apply(value);
        }
        if (modValues != null) {
            for (U modValue : modValues) {
                if (modValue != null)
                    output.submit(outputMapping.convertTo(modValue));
            }
        }
    }
}
