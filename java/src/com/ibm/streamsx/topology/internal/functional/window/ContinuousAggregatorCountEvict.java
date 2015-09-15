/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.window;

import java.util.LinkedList;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streams.operator.window.StreamWindowEvent;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionWindow;

/**
 * 
 * Continuously aggregate for a count based eviction (last(n)).
 * Only aggregates on the INSERTION event as the new tuple
 * also drives the eviction.
 * @param <I>
 *            Input tuple type
 * @param <O>
 *            Output tuple type
 */
public class ContinuousAggregatorCountEvict<I, O> extends SlidingSetAggregator<I, O> {

    public ContinuousAggregatorCountEvict(FunctionWindow op, StreamWindow<Tuple> window)
            throws Exception {
        super(op, window);
    }

    /**
     *  For a count based window, the eviction precedes the
     * insertion, but should be seen as a single action,
     * so the eviction does not result in calling the function.
     * It will be immediately followed by the INSERTION
     *which will result in the call back.
     */
    @Override
    protected void postSetUpdate(StreamWindowEvent<Tuple> event,
            Object partition, LinkedList<I> tuples) throws Exception {
        switch (event.getType()) {
        case INSERTION:
            aggregate(partition, tuples);
            break;
        default:
            break;
        }
    }
}
