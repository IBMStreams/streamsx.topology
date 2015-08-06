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
 * Periodically aggregate based upon the TRIGGER.
 * @param <I>
 *            Input tuple type
 * @param <O>
 *            Output tuple type
 */
public class PeriodicAggregator<I, O> extends SlidingSetAggregator<I, O> {

    public PeriodicAggregator(FunctionWindow op, StreamWindow<Tuple> window)
            throws Exception {
        super(op, window);
    }

    @Override
    protected void postSetUpdate(StreamWindowEvent<Tuple> event,
            Object partition, LinkedList<I> tuples) throws Exception {
        switch (event.getType()) {
        case TRIGGER:
            aggregate(partition, tuples);
            break;
        default:
            break;
        }
    }
}
