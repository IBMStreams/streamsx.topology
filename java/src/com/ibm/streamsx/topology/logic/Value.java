package com.ibm.streamsx.topology.logic;

import com.ibm.streamsx.topology.function.Supplier;

/**
 * A Supplier<T> for a single constant T value.
 * <p>
 * This class can be useful when using the Java application API
 * in the absence of Java8 Lambda expressions.
 * e.g.,
 * <pre>{@code
 *  // with Java8 Lambda expressions
 *  TStream<Foo> foo = ...
 *  foo.parallel(() -> 3).filter(...)
 *  
 *  // without Lambda expressions
 *  foo.parallel(new Value<Integer>(3)).filter(...)
 * }</pre>
 * @param <T> the value's type
 */
public class Value<T> implements Supplier<T> {
    private static final long serialVersionUID = 1L;
    private final T value;

    /**
     * 
     * @param value the value
     */
    public Value(T value) {
        this.value = value;
    }

    /**
     * @return the value
     */
    @Override
    public T get() {
        return value;
    }

}
