/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import java.util.Random;

import com.ibm.streamsx.topology.function.Predicate;

public final class RandomSample<T> implements Predicate<T> {
    private static final long serialVersionUID = 1L;
    private final double fraction;
    private final Random r = new Random();

    public RandomSample(double fraction) {
        this.fraction = fraction;
    }

    @Override
    public boolean test(T v1) {
        return r.nextDouble() <= fraction;
    }
}