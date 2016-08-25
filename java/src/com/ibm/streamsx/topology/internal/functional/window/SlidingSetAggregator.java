/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.window;

import java.util.LinkedList;
import java.util.List;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionWindow;

/**
 * This is set based aggregation, the user's function is given
 * the complete list of tuples in the window.
 * State is LinkedList<I> input tuples as their Java object, with the newest
 * tuple at the front.
 * 
 * @param <I>
 *            Input tuple type
 * @param <O>
 *            Output tuple type
 */
public abstract class SlidingSetAggregator<I, O> extends SlidingSet<I, O> {

    private FunctionalHandler<Function<List<I>, O>> aggregatorHandler;

    public SlidingSetAggregator(FunctionWindow op, StreamWindow<Tuple> window)
            throws Exception {
        super(op, window);
        aggregatorHandler = op.createLogicHandler();
    }

    protected void aggregate(Object partition, LinkedList<I> tuples)
            throws Exception {
        final Function<List<I>, O> aggregator = aggregatorHandler.getLogic();
        O aggregation = aggregator.apply(tuples);
        if (aggregation != null) {
            Tuple splTuple = outputMapping.convertTo(aggregation);
            output.submit(splTuple);
        }
    }
}
