/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;

import java.util.List;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

@PrimitiveOperator(name="Split")
@InputPortSet(cardinality = 1)
@OutputPortSet(cardinality = -1)
@Icons(location16 = "opt/icons/split_16.gif", location32 = "opt/icons/split_32.gif")
public class FunctionSplit extends FunctionFunctor {

    private FunctionalHandler<ToIntFunction<Object>> splitterHandler;
    private SPLMapping<?> mapping;
    private int n;
    private List<StreamingOutput<OutputTuple>> oports;

    @Override
    public void initialize(OperatorContext context) throws Exception {
        super.initialize(context);

        splitterHandler = createLogicHandler();
        
        OperatorContext ctxt = getOperatorContext();
        oports = ctxt.getStreamingOutputs();
        n = oports.size();
        
        mapping = getInputMapping(this, 0);
    }

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        Object value = mapping.convertFrom(tuple);

        
        final ToIntFunction<Object> splitter = splitterHandler.getLogic();
        int r;
        synchronized (splitter) {
            r = splitter.applyAsInt(value);
        }
        if (r >= 0)
            oports.get(r % n).submit(tuple);
    }
}
