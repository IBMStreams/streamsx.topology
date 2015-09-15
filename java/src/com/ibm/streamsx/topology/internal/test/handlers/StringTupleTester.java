package com.ibm.streamsx.topology.internal.test.handlers;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.tester.Condition;

public class StringTupleTester implements StreamHandler<Tuple>, Condition<String>{ 
    
    private final Predicate<String> tester;
    private String firstFailure;

    public StringTupleTester(Predicate<String> tester) {
        this.tester = tester;
    }

    @Override
    public void mark(Punctuation arg0) throws Exception {
    }

    @Override
    public void tuple(Tuple t) throws Exception {
        String tuple = t.getString(0);
        if (!tester.test(tuple)) {
            if (firstFailure == null)
                firstFailure = tuple;
        }
    }

    @Override
    public boolean valid() {
        return firstFailure == null;
    }

    @Override
    public String getResult() {
        return firstFailure;
    }

}
