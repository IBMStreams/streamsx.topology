/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.InstanceCreator;
import com.google.gson.JsonSyntaxException;
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
    
    private AbstractStreamsConnection connection;
    
    @Expose
    private String self;
    
    AbstractStreamsConnection connection() {
        return connection;
    }
    
    String self() {
        return self;
    }
    
    void setConnection(AbstractStreamsConnection connection) {
        this.connection = connection;
    }
    void setConnection(AbstractStreamsConnection connection, String self) {
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
    
    static final <E extends Element> E create(
            final AbstractStreamsConnection sc, String uri,
            Class<E> elementClass) throws IOException {
        
        E element = gson.fromJson(sc.getResponseString(uri), elementClass);
        element.setConnection(sc);
        return element;
    }
    
    /**
     * internal usage to get the list of processing elements
     * 
     */
    abstract static class ElementArray<E extends Element> {
        @Expose
        private String resourceType;
        @Expose
        private int total;

        abstract List<E> elements();
    }

    final static <E extends Element, A extends ElementArray<E>> List<E> createList(
            AbstractStreamsConnection sc,
            String uri, Class<A> arrayClass) throws IOException {
        try {
            A array = gson.fromJson(sc.getResponseString(uri), arrayClass);
            for (Element e : array.elements()) {
                e.setConnection(sc);
            }
            return array.elements();
        } catch (JsonSyntaxException e) {
            return Collections.emptyList();
        }
    }
}
