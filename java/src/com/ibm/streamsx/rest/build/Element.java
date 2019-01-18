/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest.build;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

import org.apache.http.client.fluent.Request;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.InstanceCreator;
import com.google.gson.JsonObject;
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
    
    private AbstractConnection connection;
    
    @Expose
    String self;
    
    AbstractConnection connection() {
        return connection;
    }
    
    String self() {
        return self;
    }
    
    void setConnection(AbstractConnection connection) {
        this.connection = connection;
    }
    void setConnection(AbstractConnection connection, String self) {
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
        refresh(connection().getResponseString(self));
    }
    
    private Gson refresher() {
        if (refreshJson == null) {
            refreshJson = refreshBuilder.registerTypeAdapter(getClass(),
                new InstanceCreator<Object>() {
                    @Override
                    public Object createInstance(Type arg0) {
                        return Element.this;
                    }}).create();
        }
        return refreshJson;
    }
    
    void refresh(String response) {
              
        Object me = refresher().fromJson(response, getClass());
        
        assert me == this;
    }
    void refresh(JsonObject response) {
        
        Object me = refresher().fromJson(response, getClass());
        
        assert me == this;
    }
    
    static final <E extends Element> E create(
            final AbstractConnection sc, String uri,
            Class<E> elementClass) throws IOException {
        
    	if (uri == null)
    		return null;
        return createFromResponse(sc, sc.getResponseString(uri), elementClass);
    }
    
    static final <E extends Element> E createFromResponse(
            final AbstractConnection sc, String response,
            Class<E> elementClass) throws IOException {

        E element = gson.fromJson(response, elementClass);
        element.setConnection(sc);
        return element;
    }
    
    static final <E extends Element> E createFromResponse(
            final AbstractConnection sc, JsonObject response,
            Class<E> elementClass) throws IOException {

        E element = gson.fromJson(response, elementClass);
        element.setConnection(sc);
        return element;
    }
    
    
    /**
     * internal usage to get the list of processing elements
     * 
     */
    protected abstract static class ElementArray<E extends Element> {
        @Expose
        private String resourceType;
        @Expose
        private int total;

        abstract List<E> elements();
    }

    protected final static <E extends Element, A extends ElementArray<E>> List<E> createList(
            AbstractConnection sc,
            String uri, Class<A> arrayClass) throws IOException {
    	// Assume not supported if no associated URI.
    	if (uri == null)
    		return Collections.emptyList();
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
    
    void _delete() throws IOException {
        connection().executor.execute(Request.Delete(self)).discardContent();
    }
}
