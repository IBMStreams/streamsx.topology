/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.tester.fns;

import java.util.ArrayList;
import java.util.List;

public final class TupleContents<T> extends ConditionChecker<T> {
    private static final long serialVersionUID = 1L;
    
    private final List<T> expected;
    private final boolean ordered;
    private List<T> got;

    public TupleContents(String name, boolean ordered, List<T> expected) {
        super(name);
        this.ordered = ordered;
        this.expected = expected;
        if (!ordered)
            got = new ArrayList<>(expected.size());
    }

    @Override
    public void checkValid(T tuple) {
            
        if (tupleCount() > expected.size()) {
            // too many tuples
            failTooMany(expected.size());
            return;
        }
        

        
        if (ordered)
            checkOrdered(tuple);
        else
            checkUnordered(tuple);
    }

    private void checkOrdered(T tuple) {
        int tupleIndex = (int) (tupleCount() - 1);
        
        if (tuple.equals(expected.get(tupleIndex))) {
            if (tupleCount() == expected.size())
                setValid();
        } else {
            failUnexpectedTuple(tuple, expected);
        }
    }
    
    private void checkUnordered(T tuple) {
        
        if (!expected.contains(tuple)) {
            failUnexpectedTuple(tuple, expected);
            return;
        }
        
        got.add(tuple);
        
        if (got.size() == expected.size()) {
            
            // Copy expected to avoid modifying it.
            List<T> ce = new ArrayList<>(expected.size());
            ce.addAll(expected);
            
            for (T t : got) {
                if (!ce.remove(t)) {
                    setFailed(String.format("Expected %s Received %s.", got, expected));
                    return;
                }   
            }
            setValid();
        }
    }
}
