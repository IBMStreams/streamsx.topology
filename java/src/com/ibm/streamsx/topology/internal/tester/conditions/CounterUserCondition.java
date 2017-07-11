/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions;

public final class CounterUserCondition extends UserCondition<Long> {
    
    private final long expected;
    private final boolean exact;
    
    public CounterUserCondition(long expected, boolean exact) {
        super(-1L);
        this.expected = expected;
        this.exact = exact;
    }
    
    public long getExpected() {
        return expected;
    }
    public boolean isExact() {
        return exact;
    }
    
    @Override
    public String toString() {
        return "Tuple count (exact=" + isExact() + "): " + getExpected()
                + ", received: " + getResult();
    }
}
