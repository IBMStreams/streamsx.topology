/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.tester;

/**
 * A test condition on a stream.
 * 
 * A condition is created on a stream to allow
 * testing of the contents of the stream. Conditions
 * are also used to define when a execution of a topology
 * for testing is completed, for example
 * {@link Tester#atLeastTupleCount(com.ibm.streamsx.topology.TStream, long) at least N}
 * tuples have been seen on the stream.
 * 
 * @param T Result type.
 * 
 * @see Tester#atLeastTupleCount(com.ibm.streamsx.topology.TStream, long)
 * @see Tester#tupleCount(com.ibm.streamsx.topology.TStream, long)
 * @see Tester#tupleContents(com.ibm.streamsx.topology.spl.SPLStream, com.ibm.streams.operator.Tuple...)
 * @see Tester#stringContents(com.ibm.streamsx.topology.TStream, String...)
 * @see Tester#stringContentsUnordered(com.ibm.streamsx.topology.TStream, String...)
 */
public interface Condition<T> {
    
    /**
     * Test if this condition is valid.
     * 
     * @return {@code true} if this condition is valid, {@code false} otherwise.
     */
    boolean valid();
    
    /**
     * Get the result for this condition.
     * @return result for this condition.
     */
    T getResult();
    
    /**
     * Has the condition failed.
     * 
     * Once a condition fails, it can no longer become
     * valid, for example a {@link Tester#tupleCount(com.ibm.streamsx.topology.TStream, long)}
     * fails once it the stream has received more tuples than the expected count.
     * 
     * @return True if the condition can not become valid, false otherwise.
     * 
     * @since 1.7
     */
    boolean failed();
    
    /**
     * Return a condition that is true if this AND all {@code conditions} are valid.
     * The result is a Boolean that indicates if all conditions is valid.
     * @param conditions Conditions to be ANDed together.
     * @return Condition that is valid if all {@code conditions} are valid.
     */
    default Condition<Boolean> and(final Condition<?> ...conditions) {
        if (conditions.length == 1)
            return all(this, conditions[0]);
        return and(all(conditions));
    }
    
    /**
     * Return a condition that is true if all conditions are valid.
     * The result is a Boolean that indicates if all conditions is valid.
     * @param conditions Conditions to be ANDed together.
     * @return Condition that is valid if all {@code conditions} are valid.
     */
    public static Condition<Boolean> all(final Condition<?> ...conditions) {
        return new Condition<Boolean>() {

            @Override
            public boolean valid() {
                for (Condition<?> condition : conditions) {
                    if (!condition.valid())
                        return false;
                }
                return true;
            }
            
            @Override
            public boolean failed() {
                for (Condition<?> condition : conditions) {
                    if (condition.failed())
                        return true;
                }
                return false;
            }

            @Override
            public Boolean getResult() {
                return valid();
            }};
    }
}
