/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest;

import java.io.IOException;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

/**
 * Exception for REST api wrappers
 */
public class RESTException extends IOException {

    private int status;
    private RESTErrorMessage error;

    /**
     * Customized exception that can provide more information on REST errors
     * 
     * @param code
     *            - error message code (matches HTTP response codes)
     *
     * @return a {@link RESTException} created from a code and an IBM Streams Message
     */
    public static final RESTException create(int code, String streamsMessage) {
        RESTErrorMessage error = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create().fromJson(streamsMessage,
                RESTErrorMessage.class);
        return new RESTException(code, error);
    }

    private RESTException(int code, RESTErrorMessage error) {
        super(error.getMessage());
        status = code; 
        this.error = error;
    }

    /**
     * Customized exception that can provide more information on REST errors
     * 
     * @param code
     *            - error message code (currently will contain only HTTP response codes)
     */
    public RESTException(int code) {
        super("HTTP error:" + code );
        status = code;
    }

    /**
     * Customized exception that can provide more information on REST errors
     * 
     * @param code
     *            - error message code
     * @param message
     *            - error message to be seen
     */
    public RESTException(int code, String message) {
        super(message);
        status = code;
    }

    /**
     * Customized exception that can provide more information on REST errors
     * 
     * @param message
     *            - error message to be seen
     */
    public RESTException(String message) {
        super(message);
    }

    /**
     * Gets the error status code for this exception
     * 
     * @return the error status code
     */
    public int getStatusCode() {
        return status;
    }

    /**
     * Gets the IBM Streams message ID for this exception
     * 
     * @return the IBM Streams message ID
     */

    public String getStreamsErrorMessageId() {
        String id = null ;
        if (error != null) {
            id = error.getMessageId();
        }
        return id;
    }

    /**
     * Gets the IBM Streams message for this exception as a Json Object
     * 
     * @return the IBM Streams message as a {@link JsonObject}
     */
    public JsonObject getStreamsErrorMessageAsJson() {
        JsonObject json;
        if (error != null) {
            json = error.getAsJson();
        } else {
            json = new JsonObject();
        }
        return json;
    }
}
