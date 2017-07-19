/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions.handlers;

import java.util.LinkedList;
import java.util.List;

import com.ibm.streams.flow.handlers.StreamCollector;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.internal.tester.conditions.ContentsUserCondition;

public class ContentsHandlerCondition extends HandlerCondition<List<Tuple>, StreamCollector<LinkedList<Tuple>, Tuple>, ContentsUserCondition<Tuple>> {
    
    public ContentsHandlerCondition(ContentsUserCondition<Tuple> userCondition) {
        super(userCondition, StreamCollector.newLinkedListCollector());
        
        assert userCondition.isOrdered();
    }

    @Override
    public List<Tuple> getResult() {
        return handler.getTuples();
    }

    @Override
    public boolean valid() {
        if (failed())
            return false;
               
        List<Tuple> expected = userCondition.getExpected();
        List<Tuple> got = getResult();
        
        if (expected.equals(got))
            return true;
        
        if (checkIfFailed(got, expected))
            fail();
        
        return false;
    }
    
    static <T> boolean checkIfFailed(List<T> got, List<T> expected) {
        if (expected.isEmpty())
            return false;
        for (T tuple : got) {
            if (!expected.contains(tuple))
                return true;
        }
        return false;
    }
}
