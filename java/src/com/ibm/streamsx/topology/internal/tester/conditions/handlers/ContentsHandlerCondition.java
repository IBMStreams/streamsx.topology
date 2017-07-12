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
    }

    @Override
    public List<Tuple> getResult() {
        return handler.getTuples();
    }

    @Override
    public boolean valid() {
        synchronized (handler) {
            return handler.getTuples().equals(userCondition.getExpected());
        }
    }
}
