/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions.handlers;

import static com.ibm.streamsx.topology.internal.tester.TesterRuntime.TestState.FAIL;
import static com.ibm.streamsx.topology.internal.tester.TesterRuntime.TestState.NO_PROGRESS;
import static com.ibm.streamsx.topology.internal.tester.TesterRuntime.TestState.PROGRESS;
import static com.ibm.streamsx.topology.internal.tester.TesterRuntime.TestState.VALID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.internal.tester.ConditionTesterImpl;
import com.ibm.streamsx.topology.internal.tester.TesterRuntime;
import com.ibm.streamsx.topology.internal.tester.conditions.ContentsUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.CounterUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.NoStreamCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.StringPredicateUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.UserCondition;

/**
 * Tester runtime that uses handlers to validate conditions.
 */
public abstract class HandlerTesterRuntime extends TesterRuntime {
    
    protected Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers = new HashMap<>();
    
    private final List<UserCondition<?>> allConditions = new ArrayList<>();
    
    protected HandlerTesterRuntime(ConditionTesterImpl tester) {
        super(tester);
    }
    
    @Override
    public void finalizeTester(Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers,
            Map<TStream<?>, Set<UserCondition<?>>> conditions) throws Exception {
        this.handlers.putAll(handlers);
        
        setupHandlersFromConditions(this.handlers, conditions);
        
        for (TStream<?> stream : conditions.keySet()) {
            Set<UserCondition<?>> streamConditions = conditions.get(stream);            
            allConditions.addAll(streamConditions);
        }
    }

    private void setupHandlersFromConditions(
            Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers,
            Map<TStream<?>, Set<UserCondition<?>>> conditions) {
        
        for (TStream<?> stream : conditions.keySet()) {
            Set<UserCondition<?>> streamConditions = conditions.get(stream);
            
            Set<StreamHandler<Tuple>> streamHandlers = handlers.get(stream);
            if (streamHandlers == null)
                handlers.put(stream, streamHandlers = new HashSet<>());
            
            for (UserCondition<?> userCondition : streamConditions) {
                if (stream == null) {
                    ((NoStreamCondition) userCondition).addTo(topology(), userCondition.getClass().getSimpleName());
                    continue;
                }
                
                streamHandlers.add(createHandler(userCondition));
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private StreamHandler<Tuple> createHandler(UserCondition<?> userCondition) {
        
        HandlerCondition<?,?,?> handlerCondition = null;
        
        if (userCondition instanceof CounterUserCondition) {
            handlerCondition = new CounterHandlerCondition((CounterUserCondition) userCondition);           
        } else if (userCondition instanceof ContentsUserCondition) {
            ContentsUserCondition<?> uc = (ContentsUserCondition<?>) userCondition;
            if (uc.getTupleClass().equals(Tuple.class))
                handlerCondition = new ContentsHandlerCondition((ContentsUserCondition<Tuple>) userCondition);
            else if (uc.getTupleClass().equals(String.class))
                handlerCondition = new StringHandlerCondition((ContentsUserCondition<String>) userCondition);
        } else if (userCondition instanceof StringPredicateUserCondition) {
            handlerCondition = new StringPredicateHandlerCondition((StringPredicateUserCondition) userCondition);
        }
        
        if (handlerCondition == null)
            throw new IllegalStateException();
        
        return handlerCondition.handler;
    }
    
    private Map<UserCondition<?>, Long> lastConditionState;
    
    protected final TestState testStateFromConditions(boolean complete, boolean checkCounters) {
        TestState state = VALID;
        if (checkCounters && lastConditionState == null)
            lastConditionState = new HashMap<>();
        
        for (UserCondition<?> condition : allConditions) {
            
            if (condition.failed()) {
                // fail one fail all!
                state = FAIL; 
                break;
            } else if (!condition.valid()) {
                if (complete) {
                    state = FAIL;
                    break;
                } else if (checkCounters) {
                    if (condition instanceof CounterUserCondition) {
                        CounterUserCondition counter = (CounterUserCondition) condition;
                        long result = counter.getResult();
                        Long last = lastConditionState.get(counter);
                        if (last == null || result <= last)
                            state = NO_PROGRESS;
                        else
                            state = PROGRESS;
                        
                        lastConditionState.put(counter, result);
                    } else if (condition instanceof ContentsUserCondition) {
                        ContentsUserCondition<?> contents = (ContentsUserCondition<?>) condition;
                        if (!contents.getExpected().isEmpty()) {
                            long result = contents.getResult().size();
                            Long last = lastConditionState.get(contents);
                            if (last == null || result <= last)
                                state = NO_PROGRESS;
                            else
                                state = PROGRESS;
                            lastConditionState.put(contents, result);
                        }
                    }
                }
                else
                    state = NO_PROGRESS;
            }
            
            
            if (state == FAIL)
                break;
        }
        return state;
    }
}
