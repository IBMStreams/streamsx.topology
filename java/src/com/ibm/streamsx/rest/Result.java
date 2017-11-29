/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;

/**
 * Represents a result from a REST request.
 * 
 * Any method that returns a {@code Result} defines what
 * values are returned by its methods.
 *
 * @param <T> Type of the element being created.
 * @param <R> Type of the raw response.
 * 
 * @since 1.8
 */
public interface Result<T,R> {

    /**
     * Get the object identifier associated with the request.
     * @return object identifier associated with the request.
     */
    String getId();
    
    /**
     * Get the element associated with the request.
     * @return element associated with the request.
     * @throws IOException Error communicating with REST server.
     */
    T getElement() throws IOException; 
    
    /**
     * Get the raw result from the request.
     * <BR>
     * Unless specified by the method returning
     * a {@code Result} the contents of a raw
     * result are not guaranteed to be stable
     * across releases.
     * 
     * @return raw result from the request.
     */
    R getRawResult();
    
    /**
     * Was the request successful.
     * @return {@code true} if the request was successful, {@code false} otherwise.
     */
    boolean isOk(); 
}
