/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.logic;

import com.ibm.streamsx.topology.function.Supplier;

/**
 * A Supplier<T> for a single constant T value.
 * <p>
 * This class can be useful when using the Java application API
 * in the absence of Java8 lambda expressions.
 * e.g.,
 * <pre>{@code
 *  // with Java8 Lambda expressions
 *  TStream<String> s = ...
 *  s.parallel(() -> 3).filter(...)
 *  
 *  // without Lambda expressions
 *  s.parallel(new Value<Integer>(3)).filter(...)
 *  
 *  // using the Value.of with a static import
 *  s.parallel(of(3)).filter(); 
 * }</pre>
 * @param <T> the value's type
 */
public class Value<T> implements Supplier<T> {
    private static final long serialVersionUID = 1L;
    
    /**
     * Return a constant value {@code Supplier}.
     * @param value Value of the constant.
     * @return A {@code Supplier} that always returns {@code value}.
     */
    public static <T> Supplier<T> of(T value) {
        return new Value<T>(value);
    }
    
    
    private final T value;

    /**
     * Create a constant value {@code Supplier}.
     * @param value the value
     */
    public Value(T value) {
        this.value = value;
    }

    /**
     * Return the constant value.
     * @return the value
     */
    @Override
    public T get() {
        return value;
    }
}
