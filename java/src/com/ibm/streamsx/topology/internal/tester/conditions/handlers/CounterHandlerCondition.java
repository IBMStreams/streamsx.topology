/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions.handlers;

import com.ibm.streams.flow.handlers.StreamCounter;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.internal.tester.conditions.CounterUserCondition;

public class CounterHandlerCondition extends HandlerCondition<Long, StreamCounter<Tuple>, CounterUserCondition> {
    
    public CounterHandlerCondition(CounterUserCondition userCondition) {
        super(userCondition, new StreamCounter<Tuple>());
    }
    
    @Override
    public Long getResult() {
        return handler.getTupleCount();
    }

    @Override
    public boolean valid() {
        if (failed())
            return false;
        
        if (userCondition.isExact()) {
            long count = handler.getTupleCount();
            if (count > userCondition.getExpected()) {
                fail();
                return false;
            }
            
            return count == userCondition.getExpected();
        }
        
        return handler.getTupleCount() >= userCondition.getExpected();
    }
}
