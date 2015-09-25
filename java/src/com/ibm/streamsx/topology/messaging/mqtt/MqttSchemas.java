/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.messaging.mqtt;

import static com.ibm.streams.operator.Type.Factory.getStreamSchema;

import com.ibm.streams.operator.StreamSchema;

/**
 * SPL schema for {@link com.ibm.streamsx.topology.spl.SPLStream SPLStream}
 */
class MqttSchemas {
    @SuppressWarnings("unused")
    private static final MqttSchemas forCoverage = new MqttSchemas();

    private MqttSchemas() { }

    /**
     * SPL tuple schema for the way our implementation uses the
     * com.ibm.streamsx.messaging MQTT SPL operators.
     * <p>
     * Consists of two attributes: {@code rstring topic}
     * and {@code rstring message}.
     */
    public static StreamSchema MQTT = getStreamSchema(
            "tuple<rstring topic, rstring message>");
}
