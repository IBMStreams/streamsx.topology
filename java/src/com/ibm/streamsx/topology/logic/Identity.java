/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.logic;

import com.ibm.streamsx.topology.function.UnaryOperator;

/**
 * Identity unary function that returns its input.
 *
 * @param <T> Type of the operand.
 */
public final class Identity<T> implements UnaryOperator<T> {
    private static final long serialVersionUID = 1L;

    /**
     * Returns the input value.
     * @return {@code v}
     */
    @Override
    public T apply(T v) {
        return v;
    }
}
