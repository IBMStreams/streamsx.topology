/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.window;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getLogicObject;

import java.util.LinkedList;
import java.util.List;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streamsx.topology.function7.Function;
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

    private Function<List<I>, O> aggregator;

    public SlidingSetAggregator(FunctionWindow<?> op, StreamWindow<Tuple> window)
            throws ClassNotFoundException {
        super(op, window);
        aggregator = getLogicObject(op.getFunctionalLogic());
    }

    protected void aggregate(Object partition, LinkedList<I> tuples)
            throws Exception {
        O aggregation = aggregator.apply(tuples);
        if (aggregation != null) {
            Tuple splTuple = outputMapping.convertTo(aggregation);
            output.submit(splTuple);
        }
    }
}
