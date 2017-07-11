/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions.handlers;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.internal.tester.conditions.ContentsUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.CounterUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.UserCondition;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * Condition implementation that uses a StreamHandler.
 *
 * @param <R> Return type of condition.
 * @param <H> StreamHandler type
 * @param <U> UserCondition type
 */
public abstract class HandlerCondition<R, H extends StreamHandler<Tuple>, U extends UserCondition<R>> implements Condition<R> {
    
    final U userCondition;
    final H handler;
    
    HandlerCondition(U userCondition, H handler) {
        this.handler = handler;
        this.userCondition = userCondition;
        userCondition.setImpl(this);
    }
    
    public static void setupHandlersFromConditions(
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
    
    private static StreamHandler<Tuple> createHandler(UserCondition<?> userCondition) {
        
        HandlerCondition<?,?,?> handlerCondition;
        
        if (userCondition instanceof CounterUserCondition) {
            handlerCondition = new CounterHandlerCondition((CounterUserCondition) userCondition);           
        }
        else
            throw new IllegalStateException();
        
        return handlerCondition.handler;
    }
}
