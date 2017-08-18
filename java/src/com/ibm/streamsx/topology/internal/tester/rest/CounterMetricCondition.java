/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.tester.rest;

import com.ibm.streamsx.topology.internal.tester.TesterRuntime.TestState;
import com.ibm.streamsx.topology.internal.tester.conditions.CounterUserCondition;

class CounterMetricCondition extends MetricCondition<Long> {

    CounterMetricCondition(String name, CounterUserCondition userCondition) {
        super(name, userCondition);
    }
    
    @Override
    public Long getResult() {
        if (getState() == TestState.NOT_READY)
            return -1L;
        
        return lastSeq;
    }
}
