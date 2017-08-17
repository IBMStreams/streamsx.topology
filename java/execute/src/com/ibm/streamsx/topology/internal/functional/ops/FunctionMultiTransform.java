/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getOutputMapping;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

@PrimitiveOperator(name="FlatMap")
@InputPortSet(cardinality = 1)
@OutputPortSet(cardinality = 1)
@Icons(location16 = "opt/icons/functor_16.gif", location32 = "opt/icons/functor_32.gif")
public class FunctionMultiTransform extends FunctionQueueableFunctor {

    private FunctionalHandler<Function<Object, Iterable<Object>>> transformHandler;
    private SPLMapping<Object> outputMapping;
    private StreamingOutput<OutputTuple> output;

    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);

        transformHandler = createLogicHandler();
        output = getOutput(0);
        outputMapping = getOutputMapping(this, 0);
    }

    @Override
    public void tuple(Object tuple)
            throws Exception {
        
        final Function<Object, Iterable<Object>> transform = transformHandler.getLogic();
        Iterable<Object> modValues;
        synchronized (transform) {
            modValues = transform.apply(tuple);
        }
        if (modValues != null) {
            for (Object modValue : modValues) {
                if (modValue != null)
                    output.submit(outputMapping.convertTo(modValue));
            }
        }
    }
    
    @Override
    public void mark(Punctuation mark) throws Exception {
        output.punctuate(mark);
    }
}
