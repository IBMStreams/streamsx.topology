/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional;

import com.ibm.streamsx.topology.function.Supplier;

/**
 * A specification for a value of type {@code T}
 * whose actual value is not defined until topology execution time.
 */
public class SubmissionParameter<T> implements Supplier<T> {
    private static final long serialVersionUID = 1L;
    
    private final String name;
    private final String metaType;
    private final T defaultValue;
    private transient boolean declaration;
    private transient T value;
    private transient boolean initialized;

    /**
     * A submission time parameter specification with or without a default value.
     * @param name submission parameter name
     * @param valueClass class object for {@code T}
     * @throws IllegalArgumentException if {@code name} is null or empty
     */
    public SubmissionParameter(String name, String metaType, T defaultValue) {
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("name");
        this.declaration = true;
        this.name = name;
        this.metaType = metaType;
        this.defaultValue = defaultValue;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public T get() {
        if (!initialized) {
            if (!declaration)
                value = (T) SubmissionParameterManager.getValue(name, metaType);
            initialized = true;
        }
        return value;
    }
   
    public String getName() {
        return name;
    }
    
    public T getDefaultValue() {
        return defaultValue;
    }

    public String getMetaType() {
         return metaType;
    }
}
