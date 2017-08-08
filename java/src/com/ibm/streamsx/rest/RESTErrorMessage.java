/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

/**
 * An object describing an IBM Streams REST api Error Message
 *
 */
public class RESTErrorMessage {

    @Expose
    private String id;
    @Expose
    private String message;

    public RESTErrorMessage(String id, String message) {
        this.id = id;
        this.message = message;
    }

    /**
     * Gets the error message
     * 
     * @return {@link String}
     */
    public String getMessage() {
        return message;
    }

    /**
     * Gets the id of this error message
     * 
     * @return {@link String}
     */
    public String getMessageId() {
        return id;
    }

    /**
     * Gets the json object of this error message
     * 
     * @return {@link JsonObject}
     */
    public JsonObject getAsJson() {
        JsonObject error = new JsonObject();
        error.addProperty("id", id);
        error.addProperty("message", message);
        return error;
    }

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }
}
