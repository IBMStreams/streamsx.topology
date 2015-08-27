/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.messaging.mqtt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.messaging.mqtt.Util.ParamHandler;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.tuple.Message;

/**
 * A simple connector to a MQTT broker for producing MQTT messages
 * -- publishing {@code TStream<Message>} tuples to MQTT topics.
 * <p>
 * A connector is for a specific MQTT Broker as specified in
 * the producer configuration. Any number of publishers may be created -
 * {@code publish()} may be called any number of times.
 * <p>
 * Sample use:
 * <pre>{@code
 * Topology top = ...
 * Properties producerConfig = ...
 * ProducerConnector pc = new ProducerConnector(top, producerConfig);
 * TStream<Message> msgsToPublish = ...
 * pc.publish(msgsToPublish, "myTopic");
 * }</pre>
 * <p>
 * See {@link ConsumerConnector} for configuration properties.
 * @see <a href="http://mqtt.org">http://mqtt.org</a>
 * @see <a
 *      href="http://ibmstreams.github.io/streamsx.messaging/">com.ibm.streamsx.messaging</a>
 */
public class ProducerConnector {
    @SuppressWarnings("unused")
    private final TopologyElement te;
    private final Map<String,Object> config;
    private int sinkOpCnt;
    
    private static final Map<String, ParamHandler> paramHandlers = new HashMap<>();
    static  {
        paramHandlers.put("defaultQOS", new ParamHandler("qos", Integer.class));
    }

   
    /**
     * Create a producer connector for publishing tuples.
     * <p>
     * Minimal configuration typically includes:
     * <ul>
     * <li><code>metadata.broker.list</code></li>
     * <li><code>serializer.class</code></li>
     * <li><code>request.required.acks</code></li>
     * </ul>
     *
     * @param config MQTTProducer configuration information.
     */
    public ProducerConnector(TopologyElement te, Map<String,Object> config) {
        this.te = te;
        this.config = new HashMap<>();
        this.config.putAll(config);
    }
    
    /**
     * Get the connector's producer configuration information.
     * @return the unmodifiable configuration 
     */
    public Map<String,Object> getConfig() {
        return Collections.unmodifiableMap(config);
    }

    /**
     * Publish {@code stream} tuples to the MQTT Broker.
     * Each {@code stream} tuple is sent to the topic specified by its
     * {@link Message#getTopic()} value.
     * The {@link Message#getKey()} field is ignored.
     * <p>
     * The message is handle with the quality of service indicated
     * by configuration property {@code defaultQOS}.
     * <p>
     * Same as {@code produce(stream, null, -1)}.
     * @param stream the stream to publish
     */
    public void publish(TStream<? extends Message> stream)
    {
        publish(stream, null/*topic*/, -1/*qos*/);
    }

    /**
     * Publish {@code stream} tuples to the MQTT Broker.
     * Each {@code stream} tuple is sent to the specified topic.
     * The {@link Message#getKey()} field is ignored.
     * <p>
     * The message is handle with the quality of service indicated
     * by configuration property {@code defaultQOS}.
     * <p>
     * Same as {@code produce(stream, topic, -1)}.
     * @param stream the stream to publish
     */
    public void publish(TStream<? extends Message> stream, String topic) {
        publish(stream, topic, -1);
    }
    
    /**
     * Publish {@code stream} tuples to the MQTT Broker.
     * <p>
     * If {@code topic} is null, each tuple is published to the topic
     * specified by its {@link Message#getTopic()}.
     * Otherwise, all tuples are published to {@code topic}.
     * <p>
     * The messages added to MQTT include a topic and message.
     * The {@link Message#getKey()} field is ignored.
     * <p>
     * If {@code qos==-1} the message is handled with the quality of service
     * indicated by configuration property {@code defaultQOS}.
     * 
     * @param stream the stream to publish
     * @param topic topic to publish to.  May be null.
     * @param qos quality of service. -1 to use {@code defaultQOS}.
     * 
     * @throws IllegalArgumentException if an empty {@code topic} is specified.
     * @throws IllegalArgumentException if {@code qos<-1 || qos>2}.
     */
    public void publish(TStream<? extends Message> stream, String topic, int qos) {
        if (topic!=null && topic.isEmpty())
            throw new IllegalArgumentException("topic");
        
        @SuppressWarnings("unchecked")
        SPLStream splStream = SPLStreams.convertStream((TStream<Message>)stream,
                cvtMsgFunc(topic), MqttSchemas.MQTT);
        
        Map<String,Object> params = new HashMap<String,Object>();
        params.put("reconnectionBound", -1);
        params.putAll(Util.configToSplParams(config, paramHandlers));
        if (topic == null)
            params.put("topicAttributeName", "topic");
        else
            params.put("topic", topic);
        if (qos != -1)
            params.put("qos", qos);
        params.put("dataAttributeName", "message");
        if (++sinkOpCnt > 1) {
            // each sink op requires its own clientID
            String clientId = (String) params.get("clientID");
            if (clientId != null && clientId.length() > 0)
                params.put("clientID", sinkOpCnt+"-"+clientId);
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
        Util.tagOpAsJavaPrimitive(sink.operator(), kind, className);
    }
    
    private static BiFunction<Message,OutputTuple,OutputTuple> 
            cvtMsgFunc(final String topic)
    {
        return new BiFunction<Message,OutputTuple,OutputTuple>() {
            private static final long serialVersionUID = 1L;

            @Override
            public OutputTuple apply(Message v1, OutputTuple v2) {
                v2.setString("message", toSplValue(v1.getMessage()));
                if (topic==null)
                    v2.setString("topic", toSplValue(v1.getTopic()));
                else
                    v2.setString("topic", topic);
                return v2;
            }
            
            private String toSplValue(String s) {
                // SPL doesn't allow null
                return s==null ? "" : s;
            }
        };
    }

}
