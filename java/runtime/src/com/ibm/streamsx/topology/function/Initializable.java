/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2018
 */
package com.ibm.streamsx.topology.function;

/**
 * Optional interface that a function can implement
 * to perform initialization.
 * <P>
 * {@link #initialize(FunctionContext)} is called prior
 * to the function being called to produce or process
 * stream tuples. This will be when:
 * <UL>
 * <LI>the processing element (PE) containing the function starts.</LI>
 * <LI>the PE restarts after a failure or manual request.</LI>
 * <LI>the function is restored from a checkpoint.</LI>
 * </UL>
 * </P>
 * <P>
 * Initialization is used open resources that
 * cannot be part of the function's serialized state, such
 * as files or connections to external systems.
 * Custom metrics can also be created using
 * {@link FunctionContext#createCustomMetric(String, String, String, java.util.function.LongSupplier) createCustomMetric()}.
 * <BR>
 * A function that implements this interface may also
 * implement {@code java.lang.AutoCloseable} to close
 * any resources opened in {@link #initialize(FunctionContext)}.
 * </P>
 */
public interface Initializable {
    
    /**
     * Initialize this function.
     * @param functionContext Context the function is executing in.
     * @throws Exception Exception initializing function.
     */
    void initialize(FunctionContext functionContext) throws Exception;
}
