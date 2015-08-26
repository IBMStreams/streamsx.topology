package com.ibm.streamsx.topology;

/**
 * Base class for an object of type {@code T} that can have either a 
 * compile time value or a {@link SubmissionParameter} specification.
 */
public class Value<T> {
    private final T value;
    
    protected Value() {
        value = null;
    }
    
    /**
     * A Value with a compile time value specification.
     * @param value the parameter value
     * @throws IllegalArgumentException if {@value} is null
     */
    public Value(T value) {
        if (value == null)
            throw new IllegalArgumentException("value");
        this.value = value;
    }
    
    /**
     * @return a compile time value if specified, null otherwise
     */
    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value != null ? value.toString() : "null";
    }

}