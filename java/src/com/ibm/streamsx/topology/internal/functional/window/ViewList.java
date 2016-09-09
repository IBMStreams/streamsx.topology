/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.window;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;
import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getOutputMapping;

import java.util.LinkedList;
import java.util.List;

import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.window.StatefulWindowListener;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streams.operator.window.StreamWindowEvent;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionWindow;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionParDo;

/**
 * View is a transform in Beam, which converts states of a stream into data 
 * structures. 
 *
 * <p> there are two ways to implement this in Streams:
 *
 * <UL>
 * <LI>Create a view operator for each view tranform, and then connects to 
 * downstream ParDo operators. Input tuples are maintained in the view 
 * operator. As ParDo has to map main input window into side input window with
 * {@link org.apache.beam.sdk.transforms.windowing.WindowFn#getSideInputWindow}
 * the view locally cannot know which downstream ParDo is consuming which 
 * window. Therefore, all downstream ParDo operators have to keep full states
 * of the predecessor view.</LI>
 * <LI> Each ParDo creates a ViewSet for each of its side input. The ViewSet
 * are {@link StatefulWindowListener}s inside the ParDo operator. 
 * </UL>
 *  
 * Our design goes with the second approach, as it reduces the number of view
 * state copies by one and at the same time avoiding sending data structures
 * generated from overlapping states.
 */
public class ViewList<I> extends
        StatefulWindowListener<LinkedList<I>, Tuple> {

    private static final Integer ZERO = 0;

    private final SPLMapping<I> inputMapping;
    private int port;

    public ViewList(StreamWindow<Tuple> window, FunctionParDo op, int port)
            throws ClassNotFoundException {
        super(window);
        this.port = port;
        inputMapping = getInputMapping(op, port);
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
        //case PARTITION_EVICTION:
        //    op.getnPartitions().incrementValue(-1);
        //    break;
        default:
            break;
        }
    }

    @Override
    protected final LinkedList<I> getInitializedState(Object partition,
            LinkedList<I> state) {
        return new LinkedList<I>();
    }

    // no partition for now
    public Object getPartitionKey() {
        return ZERO;
    }

    // this API should take a BoundedWindow or equivalent as a parameter,
    // and apply WindowFn.getSideInputWindow to retrieve the target time 
    // interval
    public List<I> getView() {
        return getPartitionState(getPartitionKey());
    }
}
