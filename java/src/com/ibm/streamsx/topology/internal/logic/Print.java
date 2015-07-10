/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import com.ibm.streamsx.topology.function.Consumer;

public final class Print<T> implements Consumer<T> {
    private static final long serialVersionUID = 1L;

    @Override
    public void accept(T v) {
        System.out.println(v);
    }
}