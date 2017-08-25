/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.InstanceCreator;
import com.google.gson.annotations.Expose;

/**
 * IBM Streams element.
 *
 * Super-class for elements returned from the IBM Streams REST API.
 */
public abstract class Element {
    
    static final GsonBuilder refreshBuilder = new GsonBuilder().excludeFieldsWithoutExposeAnnotation();
    static final Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
    
    private static final Gson pretty = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create();
    
    private StreamsConnectionImpl connection;
    
    @Expose
    private String self;
    
    StreamsConnectionImpl connection() {
        return connection;
    }
    
    String self() {
        return self;
    }
    
    void setConnection(StreamsConnectionImpl connection) {
        this.connection = connection;
    }
    void setConnection(StreamsConnectionImpl connection, String self) {
        this.connection = connection;
        this.self = self;
    }
    

    @Override
    public final String toString() {
        return pretty.toJson(this);
    }
    
    private Gson refreshJson;
    
    /**
     * Refresh this element.
     * 
     * Attributes of this object are updated from current state
     * though the Streams REST API.
     * 
     * @throws IOException Error communicating with Streams.
     */
    public void refresh() throws IOException {
        String response = connection().getResponseString(self);
        
        if (refreshJson == null) {
            refreshJson = refreshBuilder.registerTypeAdapter(getClass(),
                new InstanceCreator<Object>() {
                    @Override
                    public Object createInstance(Type arg0) {
                        return Element.this;
                    }}).create();
        }
        
        Object me = refreshJson.fromJson(response, getClass());
        
        assert me == this;
    }
}
