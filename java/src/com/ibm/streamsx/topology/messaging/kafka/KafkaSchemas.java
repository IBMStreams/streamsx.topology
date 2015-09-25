/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.messaging.kafka;

import static com.ibm.streams.operator.Type.Factory.getStreamSchema;

import com.ibm.streams.operator.StreamSchema;

/**
 * SPL schema for {@link com.ibm.streamsx.topology.spl.SPLStream SPLStream}
 */
class KafkaSchemas {
    @SuppressWarnings("unused")
    private static final KafkaSchemas forCoverage = new KafkaSchemas();

    private KafkaSchemas() { }

    /**
     * SPL tuple schema for the way our implementation uses the
     * com.ibm.streamsx.messaging Kafka SPL operators.
     * <p>
     * Consists of three attributes: {@code rstring topic}, {@code rstring key}
     * and {@code rstring message}.
     */
    public static StreamSchema KAFKA = getStreamSchema(
            "tuple<rstring topic, rstring key, rstring message>");
}
