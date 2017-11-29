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
     * @return raw result from the request.
     */
    R getRawResut();
}
