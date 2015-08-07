/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */

/**
 * Classes that provide the mechanisms to
 * implement functional transformations on streams.
 * <BR>
 * Note that implementations of the functional classes
 * must implement {@code java.io.Serializable}, and are always serialized and
 * deserialized before use. Thus the actual instance passed into a method
 * when declaring the {@link com.ibm.streamsx.topology.Topology} is <b>not</b> used at runtime.
 * The instance must capture any local state in its serialized state
 * to ensure the values are available at runtime.
 * <BR>
 * When an anonymous class is used as an instance then typically it
 * must be created in a static context, otherwise it will capture
 * a reference to the instance it is created from. This typically
 * is not what is required.
 * <P>
 * If the implementation of the functional class implements
 * {@link com.ibm.streamsx.topology.function.Initializable}
 * then {@link com.ibm.streamsx.topology.function.Initializable#initialize(FunctionContext)}
 * will be called when the processing element containing the function starts or restarts.
 * </P> 
 * <P>
 * If the implementation of the functional class implements
 * {@code java.lang.AutoCloseable} then the {@code close()}
 * method will be called when the application terminates.
 * </P>
 * 
 * @see com.ibm.streamsx.topology.TStream
 */
package com.ibm.streamsx.topology.function;

