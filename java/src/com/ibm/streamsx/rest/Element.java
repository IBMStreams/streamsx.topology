/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * IBM Streams element.
 *
 * Super-class for elements returned from the IBM Streams REST API.
 */
public abstract class Element {
    
    static final Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
    
    private static final Gson pretty = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create();
    
    private StreamsConnection connection;
    
    @Expose
    private String self;
    
    StreamsConnection connection() {
        return connection;
    }
    
    void setConnection(StreamsConnection connection) {
        this.connection = connection;
    }
    

    @Override
    public final String toString() {
        return pretty.toJson(this);
    }
}
