/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.window;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getLogicObject;

import java.util.LinkedList;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streams.operator.window.StreamWindow.Policy;
import com.ibm.streams.operator.window.StreamWindowEvent;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionWindow;

/**
 * 
 * State is LinkedList<I> input tuples as their Java object, with the newest
 * tuple at the front.
 * 
 * @param <I>
 *            Input tuple type
 * @param <O>
 *            Output tuple type
 */
public class SlidingSetAggregator<I, O> extends SlidingSet<I, O> {

    private Function<Iterable<I>, O> aggregator;

    public SlidingSetAggregator(FunctionWindow<?> op, StreamWindow<Tuple> window)
            throws ClassNotFoundException {
        super(op, window);
        aggregator = getLogicObject(op.getFunctionalLogic());
    }

    @Override
    protected void postSetUpdate(StreamWindowEvent<Tuple> event,
            Object partition, LinkedList<I> tuples) throws Exception {
        boolean contentChanged = false;
        switch (event.getType()) {
        case INSERTION:
            contentChanged = true;
            break;
        case EVICTION:
            // For a count based window, the eviction preceeds the
            // insertion, but should be seen as a single action,
            // so the eviction does not result in calling the function.
            // It will be immediately followed by the INSERTION
            // which will result in the call back.
            // For TIME insertion and eviction are independent.
            if (getWindow().getEvictionPolicy() == Policy.TIME)
                contentChanged = true;
            break;
        default:
            break;
        }
        if (contentChanged)
            aggregate(partition, tuples);
    }

    private void aggregate(Object partition, LinkedList<I> tuples)
            throws Exception {
        O aggregation = aggregator.apply(tuples);
        if (aggregation != null) {
            Tuple splTuple = outputMapping.convertTo(aggregation);
            output.submit(splTuple);
        }
    }
}
