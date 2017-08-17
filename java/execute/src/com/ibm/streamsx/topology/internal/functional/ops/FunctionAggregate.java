/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streams.operator.window.StreamWindow.Policy;
import com.ibm.streamsx.topology.internal.functional.window.ContinuousAggregatorCountEvict;
import com.ibm.streamsx.topology.internal.functional.window.ContinuousAggregatorTimeEvict;
import com.ibm.streamsx.topology.internal.functional.window.PeriodicAggregator;

@PrimitiveOperator(name="Aggregate")
@Icons(location16 = "opt/icons/aggregate_16.gif", location32 = "opt/icons/aggregate_32.gif")
public class FunctionAggregate<T, A> extends FunctionWindow {
    @Override
    void createWindowListener(StreamWindow<Tuple> window)
            throws Exception {
        
        if (window.getTriggerPolicy() == Policy.TIME)
            new PeriodicAggregator<T,A>(this, window);
        else if (window.getEvictionPolicy() == Policy.TIME)
            new ContinuousAggregatorTimeEvict<T,A>(this, window);
        else
            new ContinuousAggregatorCountEvict<T,A>(this, window);
    }
}
