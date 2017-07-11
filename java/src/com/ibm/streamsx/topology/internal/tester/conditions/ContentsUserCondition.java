/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions;

import java.util.Collections;
import java.util.List;

public final class ContentsUserCondition<T> extends UserCondition<List<T>> {
    
    private final List<T> expected;
    private final boolean ordered;
    
    public ContentsUserCondition(List<T> expected, boolean ordered) {
        super(Collections.emptyList());
        this.expected = expected;
        this.ordered = ordered;
    }
    
    public boolean isOrdered() {
        return ordered;
    }
    public List<T> getExpected() {
        return expected;
    }
}
