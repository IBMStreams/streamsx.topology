/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.kafka;

import java.io.Serializable;

/**
 * Stream object class usable with KafkaStreams sources.
 * 
 * @see KafkaStreams
 */
public class KafkaMessageImpl implements Serializable, KafkaMessage {
    private static final long serialVersionUID = 1L;

    private final String topic;
    private final String key;
    private final String message;

    /**
     * Create a {@code KafkaMessageImpl} that lacks a topic and a key.
     * 
     * @param message
     *            the message
     */
    public KafkaMessageImpl(String message) {
        this(message, null);
    }
    /**
     * Create a {@code KafkaMessageImpl} that lacks a topic.
     * @param message
     *            the message
     * @param key
     *            Optional key associated with the message.  May be null.
     */
    public KafkaMessageImpl(String message, String key) {
        if (message == null)
            throw new IllegalArgumentException("message==null");
        this.topic = null;
        this.key = key;
        this.message = message;
    }

    /**
     * Create a {@code KafkaMessageImpl} that includes a topic.
     * @param message
     *            the message
     * @param key
     *            Optional key associated with the message.  May be null.
     * @param topic
     *            Kafka topic identifier.
     * @throws IllegalArgumentException if topic == null
     * @throws IllegalArgumentException if message == null
     */
    public KafkaMessageImpl(String message, String key, String topic) {
        if (topic == null)
            throw new IllegalArgumentException("topic==null");
        if (message == null)
            throw new IllegalArgumentException("message==null");
        this.topic = topic;
        this.key = key;
        this.message = message;
    }
    
    @Override
    public String getKafkaTopic() {
        return topic;
    }
    
    @Override
    public String getKafkaKey() {
        return key;
    }

    @Override
    public String getKafkaMessage() {
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
        result = prime * result + message != null ? message.hashCode() : 0;
        result = prime * result + key != null ? key.hashCode() : 0;
        result = prime * result + topic != null ? topic.hashCode() : 0;
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
        KafkaMessageImpl other = (KafkaMessageImpl) obj;
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
