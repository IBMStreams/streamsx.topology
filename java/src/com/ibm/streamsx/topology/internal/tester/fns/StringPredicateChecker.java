/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.tester.fns;

import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.internal.logic.WrapperFunction;

public final class StringPredicateChecker extends ConditionChecker<Object> implements WrapperFunction {
    private static final long serialVersionUID = 1L;
    
    private final Predicate<String> predicate;

    public StringPredicateChecker(String name, Predicate<String> predicate) {
        super(name);
        this.predicate = predicate;
    }
    @Override
    public void initialize(FunctionContext functionContext) throws Exception {
        super.initialize(functionContext);
        setValid();
    }

    @Override
    void checkValid(Object v) {        
        if (!predicate.test(v.toString()))
            failed();
    }
    
    @Override
    public Object getWrappedFunction() {
        return predicate;
    }
}
