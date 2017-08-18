/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions.handlers;

import com.ibm.streamsx.topology.internal.tester.conditions.StringPredicateUserCondition;

public class StringPredicateHandlerCondition extends HandlerCondition<String, StringTupleTester, StringPredicateUserCondition> {
    
    public StringPredicateHandlerCondition(StringPredicateUserCondition userCondition) {
        super(userCondition, new StringTupleTester(userCondition.getPredicate()));
    }
    
    @Override
    public String getResult() {
        return handler.firstFailure();
    }

    @Override
    public boolean valid() {
        if (failed())
            return false;
        
        if (handler.firstFailure() == null)
            return true;
        
        fail();
        
        return false;
    }
}
