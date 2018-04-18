package com.ibm.streamsx.topology.spi.operators;

/**
 * Removes a hash added by HashAdder.
 * 
 * This is part of the SPI to allow additional functional style
 * functionality to built using the primitives provided by this toolkit.
 * 
 * In order to use parallelism the SPL toolkit using the SPL must provide
 * an Java primitive operator that extends this in its own namespace.
 * 
 * Only methods explicitly declared in this class are part of the API.
 * Use of super-class methods (except those defined by {@code java.lang.Object)}
 * is not recommended and such methods may change or be removed at any time.
 */
public abstract class HashRemover extends com.ibm.streamsx.topology.internal.functional.ops.HashRemover {
}
