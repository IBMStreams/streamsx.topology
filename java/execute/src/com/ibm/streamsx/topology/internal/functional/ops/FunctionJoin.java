/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getLogicObject;

import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.internal.functional.window.PartitionedSlidingJoin;
import com.ibm.streamsx.topology.internal.functional.window.SlidingJoin;

@PrimitiveOperator(name="Join")
@InputPorts({
        @InputPortSet(cardinality = 1, windowingMode = WindowMode.Windowed),
        @InputPortSet(cardinality = 1) })
@Icons(location16 = "opt/icons/join_16.gif", location32 = "opt/icons/join_32.gif")
public class FunctionJoin extends FunctionWindow { 
    
    private SlidingJoin<Object, Object, Object> joiner;
    
    private String joinKeyGetter;

    @Override
    void createWindowListener(StreamWindow<Tuple> window)
            throws Exception {
        if (window.isPartitioned()) {
            Function<Object,Object> joinKeyGetter = getLogicObject(getJoinKeyGetter());
            joiner = new PartitionedSlidingJoin<Object, Object, Object>(
                    this, window, joinKeyGetter);
        } else {
            joiner = new SlidingJoin<Object, Object, Object>(this, window);
        }
    }

    /**
     * Windowed tuples arrive on port 0 and thus handled by the window listener
     * Lookup tuples arrive on port 1
     */
    @Override
    public void process(StreamingInput<Tuple> stream, Tuple splTuple)
            throws Exception {

        if (stream.getPortNumber() == 1)
            joiner.port1Join(splTuple);
    }

    public String getJoinKeyGetter() {
        return joinKeyGetter;
    }

    @Parameter(optional=true)
    public void setJoinKeyGetter(String joinKeyGetter) {
        this.joinKeyGetter = joinKeyGetter;
    }
}
