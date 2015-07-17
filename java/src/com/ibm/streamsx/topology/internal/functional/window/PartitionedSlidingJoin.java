/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.window;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionWindow;
import com.ibm.streamsx.topology.tuple.Keyable;

public class PartitionedSlidingJoin<T, U, J> extends SlidingJoin<T, U, J> {

    public PartitionedSlidingJoin(FunctionWindow op,
            StreamWindow<Tuple> window) throws ClassNotFoundException {
        super(op, window);
    }

    @Override
    protected Object getPort1PartitionKey(T tTuple) {
        return ((Keyable<?>) tTuple).getKey();
    }
}
