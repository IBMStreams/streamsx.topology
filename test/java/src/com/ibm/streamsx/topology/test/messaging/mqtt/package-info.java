/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
/**
 * Tests for {@link com.ibm.streamsx.topology.messaging.mqtt.MqttStreams mqtt.MqttStreams}.
 *
 * <p>The tests require access to an operational MQTT broker with the
 * following configuration:
 * <ul>
 * <li>the broker is accessible on {@code tcp://localhost:1883}
 *  and {@code ssl://localhost:8883}</li>
 * <li>broker configured for no authentication</li>
 * </ul>
 * 
 * <p>See the {@link com.ibm.streamsx.topology.test.messaging.mqtt.MqttStreamsTest MqttStreamsTest}
 * class documentation to specify different configuration properties. 
 * 
 * <p>Information on geting and setting up a MQTT broker can be found at
 * <a href="http://mqtt.org">mqtt.org</a>
 * or
 * <a href="https://github.com/mqtt/mqtt.github.io">mqtt.github.io</a>
 * 
 * <p>For example:
 * <ul>
 * <li>download the open source Mosquitto MQTT code from
 *  <a href="http://mosquitto.org">mosquitto.org</a>, build and install it</li>
 * <li>{@code mosquitto -v &}</li>
 * <li>{@code mosquitto_sub -t testTopic1 -t testTopic2 &}</li>
 * <li>{@code mosquitto_pub -t testTopic1 -m "hello from mosquitto_pub to testTopic1"}</li>
 * <li>{@code mosquitto_pub -t testTopic2 -m "hello from mosquitto_pub to testTopic2"}</li>
 * </ul>
 */
package com.ibm.streamsx.topology.test.messaging.mqtt;

