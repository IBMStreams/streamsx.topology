/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streamsx.topology.internal.functional.window.SlidingSetAggregator;

@PrimitiveOperator
@Icons(location16 = "opt/icons/aggregate_16.gif", location32 = "opt/icons/aggregate_32.gif")
public class FunctionAggregate<T, A> extends FunctionWindow<T> {
    @Override
    void createWindowListener(StreamWindow<Tuple> window)
            throws ClassNotFoundException {
        new SlidingSetAggregator<T, A>(this, window);
    }
}
