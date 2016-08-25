/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.window;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;

import java.util.LinkedList;
import java.util.List;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streams.operator.window.StreamWindowEvent;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionWindow;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

/**
 * 
 * 
 * @param <T>
 *            Input tuple type
 * @param <J>
 *            Output (joined) tuple type
 */
public class SlidingJoin<T, U, J> extends SlidingSet<U, J> {

    private FunctionalHandler<BiFunction<T, List<U>, J>> joinerHandler;
    protected SPLMapping<T> input1Mapping;

    public SlidingJoin(FunctionWindow op, StreamWindow<Tuple> window)
            throws Exception {
        super(op, window);
        joinerHandler = op.createLogicHandler();
        input1Mapping = getInputMapping(op, 1);
    }

    /**
     * Nothing to do, the left side just looks up the list of tuples based upon
     * the partition.
     */
    @Override
    void postSetUpdate(StreamWindowEvent<Tuple> event, Object partition,
            LinkedList<U> tuples) throws Exception {
    }

    public void port1Join(Tuple splTuple) throws Exception {
        final BiFunction<T, List<U>, J> joiner = joinerHandler.getLogic();
        J jTuple;
        synchronized (this) {
            T tTuple = input1Mapping.convertFrom(splTuple);
            LinkedList<U> tuples = getPartitionState(getPort1PartitionKey(tTuple));
            jTuple = joiner.apply(tTuple, tuples);
        }
        if (jTuple != null) {
            Tuple splOutTuple = outputMapping.convertTo(jTuple);
            output.submit(splOutTuple);
        }
    }

    private static final Integer ZERO = 0;

    protected Object getPort1PartitionKey(T tTuple) {
        return ZERO;
    }
}
