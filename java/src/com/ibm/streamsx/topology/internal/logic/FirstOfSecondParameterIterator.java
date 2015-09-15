/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import java.util.List;

import com.ibm.streamsx.topology.function.BiFunction;

public class FirstOfSecondParameterIterator<T, U, R> implements
        BiFunction<T, List<U>, R>, WrapperFunction {
    private final BiFunction<T, U, R> logic;
    private static final long serialVersionUID = 6560697226858925739L;

    public FirstOfSecondParameterIterator(BiFunction<T, U, R> logic) {
        this.logic = logic;
    }

    @Override
    public R apply(T t, List<U> us) {
        if (us.isEmpty())
            return logic.apply(t, null);
        
        return logic.apply(t, us.get(0));
    }

    @Override
    public Object getWrappedFunction() {
        return logic;
    }
}
