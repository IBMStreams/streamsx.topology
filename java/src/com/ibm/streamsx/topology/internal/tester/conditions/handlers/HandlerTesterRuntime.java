/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions.handlers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.internal.tester.TesterRuntime;
import com.ibm.streamsx.topology.internal.tester.TupleCollection;
import com.ibm.streamsx.topology.internal.tester.conditions.ContentsUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.CounterUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.UserCondition;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * Tester runtime that uses handlers to validate conditions.
 */
public abstract class HandlerTesterRuntime extends TesterRuntime {
    
    protected Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers = new HashMap<>();


    
    protected HandlerTesterRuntime(TupleCollection tester) {
        super(tester);
    }
    
    @Override
    public void finalizeTester(Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers,
            Map<TStream<?>, Set<UserCondition<?>>> conditions) throws Exception {
        this.handlers.putAll(handlers);
        
        setupHandlersFromConditions(this.handlers, conditions);
    }

    private static void setupHandlersFromConditions(
            Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers,
            Map<TStream<?>, Set<UserCondition<?>>> conditions) {
        
        for (TStream<?> stream : conditions.keySet()) {
            Set<UserCondition<?>> streamConditions = conditions.get(stream);
            
            Set<StreamHandler<Tuple>> streamHandlers = handlers.get(stream);
            if (streamHandlers == null)
                handlers.put(stream, streamHandlers = new HashSet<>());
            
            for (UserCondition<?> userCondition : streamConditions)
                streamHandlers.add(createHandler(userCondition));
        }
    }
    
    @SuppressWarnings("unchecked")
    private static StreamHandler<Tuple> createHandler(UserCondition<?> userCondition) {
        
        HandlerCondition<?,?,?> handlerCondition;
        
        if (userCondition instanceof CounterUserCondition) {
            handlerCondition = new CounterHandlerCondition((CounterUserCondition) userCondition);           
        } else if (userCondition instanceof ContentsUserCondition) {
            handlerCondition = new ContentsHandlerCondition((ContentsUserCondition<Tuple>) userCondition); 
        }
        else
            throw new IllegalStateException();
        
        return handlerCondition.handler;
    }
}
