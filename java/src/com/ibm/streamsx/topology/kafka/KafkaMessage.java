package com.ibm.streamsx.topology.kafka;

import java.io.Serializable;

/**
 * The components of an Apache Kafka Message.
 */
public interface KafkaMessage extends Serializable {

    /**
     * Get the Kafka Message content for this object.
     * @return message the message
     */
    String getKafkaMessage();

    /**
     * Get the Kafka Message key for this object.
     * @return optional message key. May be null.
     */
    String getKafkaKey();

    /**
     * Get the Kafka Message topic for this object.
     * @return optional message topic. May be null.
     */
    String getKafkaTopic();

}