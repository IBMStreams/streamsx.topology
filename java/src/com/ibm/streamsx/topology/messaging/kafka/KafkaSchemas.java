/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.messaging.kafka;

import static com.ibm.streams.operator.Type.Factory.getStreamSchema;

import com.ibm.streams.operator.StreamSchema;

/**
 * SPL schema for tuples associated with com.ibm.streamsx.messaging toolkit
 * SPL {@code KafkaConsumer} and {@code KafkaProducer} operators.
 */
public class KafkaSchemas {

    private KafkaSchemas() {
    }

    /**
     * SPL schema used for {@link com.ibm.streamsx.topology.spl.SPLStream SPLStream}
     * of Kafka messages.
     * Consists of three attributes: {@code rstring topic}, {@code rstring key}
     * and {@code rstring message}.
     */
    public static StreamSchema KAFKA = getStreamSchema(
            "tuple<rstring topic, rstring key, rstring message>");
}
