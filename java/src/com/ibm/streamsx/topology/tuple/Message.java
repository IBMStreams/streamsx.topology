package com.ibm.streamsx.topology.tuple;

import java.io.Serializable;

/**
 * An interface for tuple attributes common to messaging adapters
 * in {@code com.ibm.streamsx.topology.messaging}.
 */
public interface Message extends Serializable {

    /**
     * Get the message content for this object.
     * @return message the message
     */
    String getMessage();

    /**
     * Get the message key for this object.
     * @return optional message key. May be null.
     */
    String getKey();

    /**
     * Get the message topic for this object.
     * @return optional message topic. May be null.
     */
    String getTopic();

}