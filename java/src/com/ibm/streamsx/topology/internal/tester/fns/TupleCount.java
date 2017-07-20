/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.tester.fns;

import com.ibm.streamsx.topology.function.FunctionContext;

public final class TupleCount<T> extends ConditionChecker<T> {
    private static final long serialVersionUID = 1L;
    
    private final long expected;
    private final boolean exact;

    public TupleCount(String name, long expected, boolean exact) {
        super(name);
        this.expected = expected;
        this.exact = exact;
    }
    @Override
    public void initialize(FunctionContext functionContext) throws Exception {
        super.initialize(functionContext);
        if (expected == 0)
            setValid();
    }

    @Override
    void checkValid(T v) {        
        if (tupleCount() == expected)
            setValid();
        else if (exact && tupleCount() > expected)
            // can never become valid again
            failTooMany(expected);
    }
}
