/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
/**
 * Support for integrating with the MQTT messaging system
 * <a href="http://mqtt.org">http://mqtt.org</a>.
 * <p>
 * {@link com.ibm.streamsx.topology.messaging.mqtt.MqttStreams MqttStreams} is
 * a connector to bridge between topology streams
 * and a MQTT broker.
 * <p>
 * Functionality in this package is provided by the open source SPL toolkit
 * <a href="http://ibmstreams.github.io/streamsx.messaging/">com.ibm.streamsx.messaging</a>
 * release 2.0.0 or later.
 * This requires that the {@code com.ibm.streamsx.messaging} toolkit 
 * is made available when submitting the application, by either:
 * <ul>
 * <li>Submitting to an IBM Streams 4.0 or later instance.</LI>
 * <li>Including the toolkit in the toolkit path specified by the environment variable {@code STREAMS_SPLPATH}</li>
 * <li>Specifically adding the toolkit using {@link com.ibm.streamsx.topology.spl.SPL#addToolkit(com.ibm.streamsx.topology.TopologyElement, java.io.File)}. </li>
 * </ul>
 * Releases of the toolkit are available from:
 *  <a href="https://github.com/IBMStreams/streamsx.messaging/releases" target="_blank">https://github.com/IBMStreams/streamsx.messaging/releases</a>. 
 */
package com.ibm.streamsx.topology.messaging.mqtt;
