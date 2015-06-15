/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import java.util.Iterator;

import com.ibm.streamsx.topology.function7.BiFunction;

public class FirstOfSecondParameterIterator<T, U, R> implements
        BiFunction<T, Iterable<U>, R>, WrapperFunction {
    private final BiFunction<T, U, R> logic;
    private static final long serialVersionUID = 6560697226858925739L;

    public FirstOfSecondParameterIterator(BiFunction<T, U, R> logic) {
        this.logic = logic;
    }

    @Override
    public R apply(T t, Iterable<U> us) {
        Iterator<U> it = us.iterator();
        if (it.hasNext()) {
            return logic.apply(t, it.next());
        }
        return null;
    }

    @Override
    public Object getWrappedFunction() {
        return logic;
    }
}
