/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions;

import com.ibm.streamsx.topology.function.Predicate;

public final class StringPredicateUserCondition extends UserCondition<String> {
    
    private final Predicate<String> predicate;
    
    public StringPredicateUserCondition(Predicate<String> predicate) {
        super(null);
        this.predicate = predicate;
    }
    
    public Predicate<String> getPredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return "Predicate: " + predicate;
    }
}