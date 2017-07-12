/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions.handlers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.ibm.streams.flow.handlers.StreamCollector;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.internal.tester.conditions.ContentsUserCondition;

public class StringHandlerCondition extends HandlerCondition<List<String>, StreamCollector<LinkedList<Tuple>, Tuple>, ContentsUserCondition<String>> {
    
    public StringHandlerCondition(ContentsUserCondition<String> userCondition) {
        super(userCondition, StreamCollector.newLinkedListCollector());
    }

    @Override
    public List<String> getResult() {
        List<String> strings = new ArrayList<>(handler.getTupleCount());
        synchronized (handler.getTuples()) {
            for (Tuple tuple : handler.getTuples()) {
                strings.add(tuple.getString(0));
            }
        }
        return strings;
    }

    @Override
    public boolean valid() {
        List<String> expected = userCondition.getExpected();
        if (expected.size() != handler.getTupleCount())
            return false;

        List<String> got = getResult();
        if (expected.size() != got.size())
            return false;
        
        if (userCondition.isOrdered())
            return expected.equals(got);
        
        // don't modify the original expected.
        expected = new ArrayList<>(expected);
        
        Collections.sort(expected);
        Collections.sort(got);
        
        return expected.equals(got);
    }
}
