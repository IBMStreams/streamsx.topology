/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.tuple;

import java.io.Serializable;

/**
 * A Simple implementation of an immutable {@link Message}.
 */
public class SimpleMessage implements Serializable, Message {
    private static final long serialVersionUID = 1L;

    private final String topic;
    private final String key;
    private final String message;

    /**
     * Create a {@code SimpleMessage} that lacks a key and topic.
     * 
     * @param message
     *            the message
     */
    public SimpleMessage(String message) {
        this(message, null, null);
    }
    
    /**
     * Create a {@code SimpleMessage} that lacks a topic.
     * @param message
     *            the message
     * @param key
     *            Optional key associated with the message.  May be null.
     */
    public SimpleMessage(String message, String key) {
        this(message, key, null);
    }

    /**
     * Create a {@code SimpleMessage}.
     * @param message
     *            the message
     * @param key
     *            Optional key associated with the message.  May be null.
     * @param topic
     *            Optional topic identifier associated with the message.
     *            May be null.
     * @throws IllegalArgumentException if message == null
     */
    public SimpleMessage(String message, String key, String topic) {
        if (message == null)
            throw new IllegalArgumentException("message==null");
        this.topic = topic;
        this.key = key;
        this.message = message;
    }
    
    @Override
    public String getTopic() {
        return topic;
    }
    
    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "{topic=" + topic + ", key=" + key + ", message=" + message + "}";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (message != null ? message.hashCode() : 0);
        result = prime * result + (key != null ? key.hashCode() : 0);
        result = prime * result + (topic != null ? topic.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleMessage other = (SimpleMessage) obj;
        if (message != other.message
            && (message == null || !message.equals(other.message))) {
                return false;
        }
        if (key != other.key
            && (key == null || !key.equals(other.key))) {
                return false;
        }
        if (topic != other.topic
            && (topic == null || !topic.equals(other.topic))) {
                return false;
        }
        return true;
    }
}
