/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.messaging.mqtt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.logic.Value;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tuple.Message;
import com.ibm.streamsx.topology.tuple.SimpleMessage;

/**
 * A simple connector to an MQTT broker for consuming MQTT messages
 * -- subscribing to MQTT topics and creating a {@code TStream<Message>}.
 * <p>
 * A connector is for a specific MQTT broker as specified in
 * the consumer configuration.  Any number of subscribers may be created -
 * {@code subscribe()} may be called any number of times.
 * <p>
 * Sample use:
 * <pre>{@code
 * Topology top = new Topology("An MQTT application");
 * // create submission properties for configuration information
 * Supplier<T> serverID = top.createSubmissionParameter("mqtt.serverID", "tcp://localhost:1883");
 * Supplier<T> userID = top.createSubmissionParameter("mqtt.userID", System.getProperty("user.name"));
 * Supplier<T> password = top.createSubmissionParameter("mqtt.password", String.class);
 * Supplier<T> topic = top.createSubmissionParameter("mqtt.topic", String.class);
 * 
 * // create the connector's configuration property map
 * Map<String,Object> config = new HashMap<>();
 * config.put("serverID", serverID);
 * config.put("userID", userID);
 * config.put("password", password);
 * 
 * // create the connector
 * ConsumerConnector consumer = new ConsumerConnector(top, config);
 * 
 * // subscribe to the submission parameter topic
 * TStream<Message> rcvdMsgs = consumer.subscribe(topic);
 * rcvdMsgs.print();
 * 
 * // subscribe to a compile time topic
 * // with Java8 Lambda expression...
 * TStream<Message> rcvdMsgs2 = consumer.subscribe(()->"anotherTopic");
 * // without Java8...
 * TStream<Message> rcvdMsgs2 = consumer.subscribe(new Value("anotherTopic"));
 * }</pre>
 * <p>
 * Configuration properties apply to {@code ConsumerConnector} and
 * {@code ProducerConnector} configurations unless stated otherwise.
 * <br>
 * All properties may be specified as submission parameters unless
 * stated otherwise.  
 * See {@link Topology#createSubmissionParameter(String, Class)}.
 * <p>
 * <table border=1>
 * <tr><th>Property</th><th>Description</th></tr>
 * <tr><td>serverURI</td>
 *      <td>Required String. URI to the MQTT server, either
 *      {@code tcp://<hostid>[:<port>]}
 *      or {@code ssl://<hostid>[:<port>]}. 
 *      The port defaults to 1883 for "tcp:" and 8883 for "ssl:" URIs.
 *      </td></tr>
 * <tr><td>clientID</td>
 *      <td>Optional String. A unique identifier for a connection
 *      to the MQTT server. By default a unique client ID is automatically
 *      generated for each connection.  The MQTT broker only allows a single
 *      connection for a particular {@code clientID}.
 *      </td></tr>
 * <tr><td>keepAliveInterval</td>
 *      <td>Optional Integer.  Automatically generate a MQTT
 *      ping message to the server if a message or ping hasn't been
 *      sent or received in the last keelAliveInterval seconds.  
 *      Enables the client to detect if the server is no longer available
 *      without having to wait for the TCP/IP timeout.  
 *      A value of 0 disables keepalive processing.
 *      The default is 60.
 *      </td></tr>
 * <tr><td>commandTimeoutMsec</td>
 *      <td>Optional Long. The maximum time in milliseconds
 *      to wait for a MQTT connect or publish action to complete.
 *      A value of 0 causes the client to wait indefinitely.
 *      The default is 0.
 *      </td></tr>
 * <tr><td>reconnectDelayMsec</td>
 *      <td>Optional Long. The time in milliseconds before
 *      attempting to reconnect to the server following a connection failure.
 *      The default is 60000.
 *      </td></tr>
 * <tr><td>userID</td>
 *      <td>Optional String.  The identifier to use when authenticating
 *      with a server configured to require that form of authentication.
 *      </td></tr>
 * <tr><td>password</td>
 *      <td>Optional String.  The identifier to use when authenticating
 *      with server configured to require that form of authentication.
 *      </td></tr>
 * <tr><td>trustStore</td>
 *      <td>Optional String. The pathname to a file containing the
 *      public certificate of trusted MQTT servers.  If a relative path
 *      is specified, the path is relative to the application directory.
 *      Required when connecting to a MQTT server with an 
 *      ssl:/... serverURI.
 *      </td></tr>
 * <tr><td>trustStorePassword</td>
 *      <td>Required String when {@code trustStore} is used.
 *      The password needed to access the encrypted trustStore file.
 *      </td></tr>
 * <tr><td>keyStore</td>
 *      <td>Optional String. The pathname to a file containing the
 *      MQTT client's public private key certificates.
 *      If a relative path is specified, the path is relative to the
 *      application directory. 
 *      Required when an MQTT server is configured to use SSL client authentication.
 *      </td></tr>
 * <tr><td>keyStorePassword</td>
 *      <td>Required String when {@code keyStore} is used.
 *      The password needed to access the encrypted keyStore file.
 *      </td></tr>
 * <tr><td>receiveBufferSize</td>
 *      <td>[consumer] Optional Integer. The size, in number
 *      of messages, of the consumer's internal receive buffer.  Received
 *      messages are added to the buffer prior to being converted to a
 *      stream tuple. The receiver blocks when the buffer is full.
 *      The default is 50.
 *      </td></tr>
 * <tr><td>retain</td>
 *      <td>[producer] Optional Boolean. Indicates if messages should be
 *      retained on the MQTT server.  Default is false.
 *      </td></tr>
 * <tr><td>defaultQOS</td>
 *      <td>Optional Integer. The default
 *      MQTT quality of service used for message handling.
 *      The default is 0.
 *      </td></tr>
 * </table>
 * 
 * @see <a href="http://mqtt.org">http://mqtt.org</a>
 * @see <a
 *      href="http://ibmstreams.github.io/streamsx.messaging/">com.ibm.streamsx.messaging</a>
 */
public class ConsumerConnector {
    private final TopologyElement te;
    private final Map<String,Object> config;
    private int sourceOpCnt;

    /**
     * Create a consumer connector for subscribing to topics.
     * <p>
     * @param te {@link TopologyElement} 
     * @param config consumer configuration property information.
     */
    public ConsumerConnector(TopologyElement te, Map<String, Object> config) {
        this.te = te;
        this.config = new HashMap<>();
        this.config.putAll(config);
    }
    
    /**
     * Get the connector's configuration information.
     * @return the unmodifiable configuration 
     */
    public Map<String,Object> getConfig() {
        return Collections.unmodifiableMap(config);
    }
    
    /**
     * Subscribe to a MQTT topic and create a stream of messages.
     * <p>
     * The quality of service for handling each topic is
     * the value of configuration property {@code defaultQOS}.
     * <p> 
     * N.B., A topology that includes this will not support
     * {@code StreamsContext.Type.EMBEDDED}.
     * <p>
     * N.B. due to com.ibm.streamsx.messaging 
     * <a href="https://github.com/IBMStreams/streamsx.messaging/issues/124">issue#124</a>,
     * terminating a {@code StreamsContext.Type.STANDALONE} topology may result
     * in ERROR messages and a stranded standalone process.

     * @param topic the MQTT topic.  May be a submission parameter.
     * @return TStream&lt;Message>
     *      The generated {@code Message} tuples have a non-null {@code topic}.
     *      The tuple's {@code key} will be null. 
     * @throws IllegalArgumentException if topic is null.
     * @see Value
     * @see Topology#createSubmissionParameter(String, Class)
    */
    public TStream<Message> subscribe(Supplier<String> topic)
    {
        if (topic==null)
            throw new IllegalArgumentException("topic");

        Map<String, Object> params = new HashMap<>();
        params.put("reconnectionBound", -1);
        params.put("qos", 0);
        params.putAll(Util.configToSplParams(config));
        params.put("topics", topic);
        params.put("topicOutAttrName", "topic");
        params.put("dataAttributeName", "message");
        if (++sourceOpCnt > 1) {
            // each source op requires its own clientID
            String clientId = (String) params.get("clientID");
            if (clientId != null && clientId.length() > 0)
                params.put("clientID", sourceOpCnt+"-"+clientId);
        }

        // Use SPL.invoke to avoid adding a compile time dependency
        // to com.ibm.streamsx.messaging since JavaPrimitive.invoke*()
        // lack "kind" based variants.
        String kind = "com.ibm.streamsx.messaging.mqtt::MQTTSource";
        String className = "com.ibm.streamsx.messaging.mqtt.MqttSourceOperator";
        SPLStream rawMqtt = SPL.invokeSource(
                        te,
                        kind,
                        params,
                        MqttSchemas.MQTT);
        SPL.tagOpAsJavaPrimitive(toOp(rawMqtt), kind, className);
        
        TStream<Message> rcvdMsgs = toMessageStream(rawMqtt);
        rcvdMsgs.colocate(rawMqtt);
        return rcvdMsgs;
    }
    
    private BOperatorInvocation toOp(SPLStream splStream) {
        BOutputPort oport = (BOutputPort) splStream.output();
        return (BOperatorInvocation) oport.operator();
    }

    /**
     * Convert an {@link SPLStream} with schema {@link MqttSchemas.MQTT}
     * to a TStream&lt;{@link Message}>.
     * The returned stream will contain a {@code Message} tuple for
     * each tuple on {@code stream}.
     * A runtime error will occur if the schema of {@code stream} doesn't
     * have the attributes defined by {@code MqttSchemas.MQTT}.
     * @param stream Stream to be converted to a TStream&lt;Message>.
     * @return Stream of {@code Message} tuples from {@code stream}.
     */
    private static TStream<Message> toMessageStream(SPLStream stream) {
        return stream.convert(new Function<Tuple, Message>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Message apply(Tuple tuple) {
                return new SimpleMessage(tuple.getString("message"),
                                    null, // key
                                    tuple.getString("topic"));
            }
        });
    }

}
