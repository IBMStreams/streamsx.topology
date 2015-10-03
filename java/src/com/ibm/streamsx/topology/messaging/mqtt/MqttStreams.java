/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.messaging.mqtt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.logic.Value;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.tuple.Message;
import com.ibm.streamsx.topology.tuple.SimpleMessage;

/**
 * A simple connector to a MQTT broker for publishing
 * {@code TStream<Message>} tuples to MQTT topics, and
 * subscribing to MQTT topics and creating {@code TStream<Message>} streams.
 * <p>
 * A connector is for a specific MQTT Broker as specified in
 * the configuration. Any number of {@code publish()} and {@code subscribe()}
 * connections may be created from a single MqttStreams connector.
 * <p>
 * Sample use:
 * <pre>{@code
 * Topology t = new Topology("An MQTT application");
 * // optionally, define submission properties for configuration information
 * Supplier<T> serverID = t.createSubmissionParameter("mqtt.serverID", "tcp://localhost:1883");
 * Supplier<T> userID = t.createSubmissionParameter("mqtt.userID", System.getProperty("user.name"));
 * Supplier<T> password = t.createSubmissionParameter("mqtt.password", String.class);
 * Supplier<T> pubTopic = t.createSubmissionParameter("mqtt.pubTopic", String.class);
 * Supplier<T> subTopic = t.createSubmissionParameter("mqtt.subTopic", String.class);
 * 
 * // create the connector's configuration property map
 * Map<String,Object> config = new HashMap<>();
 * config.put("serverID", serverID);
 * config.put("userID", userID);
 * config.put("password", password);
 * 
 * // create the connector
 * MqttStreams mqtt = new MqttStreams(t, config);
 * 
 * // publish to the submission parameter "pubTopic"
 * TStream<Message> msgsToPublish = ...
 * mqtt.publish(msgsToPublish, pubTopic);
 * 
 * // publish to a compile time topic
 * // with Java8 Lambda expression...
 * mqtt.publish(msgsToPublish, ()->"anotherTopic");
 * // without Java8...
 * mqtt.publish(msgsToPublish, new Value("anotherTopic"));
 * 
 * // subscribe to the submission parameter "subTopic"
 * TStream<Message> rcvdMsgs = mqtt.subscribe(subTopic);
 * rcvdMsgs.print();
 * 
 * // subscribe to a compile time topic
 * // with Java8 Lambda expression...
 * TStream<Message> rcvdMsgs2 = mqtt.subscribe(()->"anotherTopic");
 * // without Java8...
 * TStream<Message> rcvdMsgs2 = mqtt.subscribe(new Value("anotherTopic"));
 * }</pre>
 * <p>
 * Configuration properties apply to {@code publish} and
 * {@code subscribe} unless stated otherwise.
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
 *      to the MQTT server. 
 *      The MQTT broker only allows a single
 *      connection for a particular {@code clientID}.
 *      By default a unique client ID is automatically
 *      generated for each use of {@code publish()} and {@code subscribe()}.
 *      The specified clientID is used for the first
 *      use {@code publish()} or {@code subscribe()} use and
 *      suffix is added for each subsequent uses.
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
 *      <td>[subscribe] Optional Integer. The size, in number
 *      of messages, of the subscriber's internal receive buffer.  Received
 *      messages are added to the buffer prior to being converted to a
 *      stream tuple. The receiver blocks when the buffer is full.
 *      The default is 50.
 *      </td></tr>
 * <tr><td>retain</td>
 *      <td>[publish] Optional Boolean. Indicates if messages should be
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
public class MqttStreams {
    private final TopologyElement te;
    private final Map<String,Object> config;
    private int opCnt;
    
    /**
     * Create a MQTT connector for publishing tuples to topics
     * subscribing to topics.
     * <p>
     * @param te {@link TopologyElement} 
     * @param config configuration property information.
     */
    public MqttStreams(TopologyElement te, Map<String, Object> config) {
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
     * Publish {@code stream} tuples to one or more MQTT topics.
     * <p>
     * Each {@code stream} tuple is sent to the topic specified by its
     * {@link Message#getTopic()} value.
     * The {@link Message#getKey()} field is ignored.
     * <p>
     * The message is handled with the quality of service indicated
     * by configuration property {@code defaultQOS}.
     * <p>
     * Same as {@code publish(stream, null)}.
     * @param stream the stream to publish
     * @return the sink element
     */
    public TSink publish(TStream<? extends Message> stream)
    {
        return publish(stream, null/*topic*/);
    }
    
    /**
     * Publish {@code stream} tuples to one or more MQTT topics.
     * <p>
     * If {@code topic} is null, each tuple is published to the topic
     * specified by its {@link Message#getTopic()}.
     * Otherwise, all tuples are published to {@code topic}.
     * <p>
     * The messages added to MQTT include a topic and message.
     * The {@link Message#getKey()} field is ignored.
     * <p>
     * The message is handled with the quality of service
     * indicated by configuration property {@code defaultQOS}.
     * 
     * @param stream the stream to publish
     * @param topic topic to publish to.  May be a submission parameter. May be null.
     * @return the sink element
     * @see Value
     * @see Topology#createSubmissionParameter(String, Class)
     */
    public TSink publish(TStream<? extends Message> stream, Supplier<String> topic) {
        
        stream = stream.lowLatency();
        
        @SuppressWarnings("unchecked")
        SPLStream splStream = SPLStreams.convertStream((TStream<Message>)stream,
                cvtMsgFunc(topic), MqttSchemas.MQTT);
        
        Map<String,Object> params = new HashMap<String,Object>();
        params.put("reconnectionBound", -1);
        params.put("qos", 0);
        params.putAll(Util.configToSplParams(config));
        params.remove("messageQueueSize");
        if (topic == null)
            params.put("topicAttributeName", "topic");
        else
            params.put("topic", topic);
        params.put("dataAttributeName", "message");
        if (++opCnt > 1) {
            // each op requires its own clientID
            String clientId = (String) params.get("clientID");
            if (clientId != null && clientId.length() > 0)
                params.put("clientID", opCnt+"-"+clientId);
        }
       
        // Use SPL.invoke to avoid adding a compile time dependency
        // to com.ibm.streamsx.messaging since JavaPrimitive.invoke*()
        // lack "kind" based variants.
        String kind = "com.ibm.streamsx.messaging.mqtt::MQTTSink";
        String className = "com.ibm.streamsx.messaging.kafka.MqttSinkOperator";
        TSink sink = SPL.invokeSink(
                kind,
                splStream,
                params);
        SPL.tagOpAsJavaPrimitive(sink.operator(), kind, className);
        return sink;
    }
    
    private static BiFunction<Message,OutputTuple,OutputTuple> 
            cvtMsgFunc(final Supplier<String> topic)
    {
        return new BiFunction<Message,OutputTuple,OutputTuple>() {
            private static final long serialVersionUID = 1L;

            @Override
            public OutputTuple apply(Message v1, OutputTuple v2) {
                v2.setString("message", toSplValue(v1.getMessage()));
                if (topic==null)
                    v2.setString("topic", toSplValue(v1.getTopic()));
                return v2;
            }
            
            private String toSplValue(String s) {
                // SPL doesn't allow null
                return s==null ? "" : s;
            }
        };
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
     *
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
        params.remove("retain");
        params.put("topics", topic);
        params.put("topicOutAttrName", "topic");
        params.put("dataAttributeName", "message");
        if (++opCnt > 1) {
            // each op requires its own clientID
            String clientId = (String) params.get("clientID");
            if (clientId != null && clientId.length() > 0)
                params.put("clientID", opCnt+"-"+clientId);
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
