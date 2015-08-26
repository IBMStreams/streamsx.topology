package com.ibm.streamsx.topology;

/**
 * A object of type {@code T} that can have either a static value or
 * a {@link SubmissionParameter} specification. 
 */
public class Parameter<T> {
    public final T value;
    public final SubmissionParameter<T> sp;
    
    /**
     * A Parameter with a static value specification.
     * @param value the parameter value
     */
    public Parameter(T value) {
        this.value = value;
        this.sp = null;
    }
    
    /**
     * A Parameter with a {@code SubmissionParameter} value specification.
     * @param value the parameter value
     */
    public Parameter(SubmissionParameter<T> value) {
        this.value = null;
        this.sp = value;
    }

    @Override
    public String toString() {
        return (value != null ? value : sp).toString();
    }

}