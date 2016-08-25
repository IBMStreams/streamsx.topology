/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.window;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;
import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getOutputMapping;

import java.util.LinkedList;

import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.window.StatefulWindowListener;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streams.operator.window.StreamWindowEvent;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionWindow;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

/**
 * 
 * State is LinkedList<I> input tuples as their Java object, with the oldest
 * tuple first.
 * 
 * @param <I>
 *            Input tuple type
 */
public abstract class SlidingSet<I, O> extends
        StatefulWindowListener<LinkedList<I>, Tuple> {

    private final FunctionWindow op;
    private final SPLMapping<I> inputMapping;

    protected final SPLMapping<O> outputMapping;
    protected final StreamingOutput<?> output;

    protected SlidingSet(FunctionWindow op, StreamWindow<Tuple> window)
            throws ClassNotFoundException {
        super(window);
        this.op = op;
        inputMapping = getInputMapping(op, 0);
        output = op.getOutput(0);
        outputMapping = getOutputMapping(op, 0);
    }

    @Override
    public synchronized final void handleEvent(StreamWindowEvent<Tuple> event)
            throws Exception {
        final Object partition = event.getPartition();
        LinkedList<I> tuples = getPartitionState(partition);

        switch (event.getType()) {
        case INSERTION:
            for (Tuple splTuple : event.getTuples()) {
                I tuple = inputMapping.convertFrom(splTuple);
                tuples.addLast(tuple);
            }

            break;
        case EVICTION:
            // we only support count and time based eviction, which
            // means any eviction is always the oldest N tuples.
            for (@SuppressWarnings("unused") Tuple splTuple : event.getTuples()) {
                tuples.removeFirst();
            }
            break;
        case PARTITION_EVICTION:
            op.getnPartitions().incrementValue(-1);
            break;
        default:
            break;
        }

        postSetUpdate(event, partition, tuples);
    }

    abstract void postSetUpdate(StreamWindowEvent<Tuple> event,
            Object partition, LinkedList<I> tuples) throws Exception;

    @Override
    protected final LinkedList<I> getInitializedState(Object partition,
            LinkedList<I> state) {
        op.getnPartitions().increment();
        return new LinkedList<I>();
    }
}
