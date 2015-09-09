/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.messaging.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.tuple.Message;

/**
 * A simple connector to an Apache Kafka cluster for producing Kafka messages
 * -- publishing a {@code TStream<Message>} to Kafka topics.
 * <p>
 * A single connector is for a specific Kafka Broker as specified in
 * the producer configuration.
 * <p> 
 * A connector can create any number of producers in the topology.  
 * A producer can publish to one or more topics.
 * <p>
 * Sample use:
 * <pre>
 * Topology top = ...
 * Properties producerConfig = ...
 * ProducerConnector pc = new ProducerConnector(top, producerConfig);
 *  
 * TStream<MyType> myStream = ...
 * TStream<Message> msgsToSend = myStream.transform(MyType to SimpleMessage);
 * pc.publish(msgsToSend, "myTopic");
 * </pre>
 * 
 * @see <a href="http://kafka.apache.org">http://kafka.apache.org</a>
 * @see <a
 *      href="http://ibmstreams.github.io/streamsx.messaging/">com.ibm.streamsx.messaging</a>
 */
public class ProducerConnector {
    private static final String PROP_FILE_PARAM = "etc/kafkaStreams/emptyProducerProperties";
    private final TopologyElement te;
    private final Map<String,String> config;
    private boolean addedFileDependency;
    
    void addPropertiesFile() {
        if (!addedFileDependency) {
            addedFileDependency = true;
            Util.addPropertiesFile(te, PROP_FILE_PARAM);
        }
    }
   
    /**
     * Create a producer connector for publishing tuples.
     * <p>
     * See the Apache Kafka documentation for {@code KafkaProducer}
     * configuration properties at <a href="http://kafka.apache.org">http://kafka.apache.org</a>.
     * <p>
     * Minimal configuration typically includes:
     * <ul>
     * <li><code>metadata.broker.list</code></li>
     * <li><code>serializer.class</code></li>
     * <li><code>request.required.acks</code></li>
     * </ul>
     *
     * @param config KafkaProducer configuration information.
     */
    public ProducerConnector(TopologyElement te, Properties config) {
        this.te = te;
        this.config = Util.toMap(config);
    }
    
    /**
     * Get the connector's {@code KafkaProducer} configuration information.
     * @return the unmodifiable configuration 
     */
    public Map<String,String> getConfig() {
        return Collections.unmodifiableMap(config);
    }

    /**
     * Publish {@code stream} tuples to the Kafka Broker.
     * Each {@code stream} tuple is sent to the topic specified by its
     * {@link Message#getTopic()}.
     * Same as {@code produce(stream, null)}.
     * @param stream the stream to publish
     * @return the sink element
     */
    public TSink publish(TStream<? extends Message> stream)
    {
        return publish(stream, null/*topic*/);
    }

    /**
     * Publish {@code stream} tuples to the Kafka Broker.
     * <p>
     * If {@code topic} is null, each tuple is published to the topic
     * specified by its {@link Message#getTopic()}.
     * Otherwise, all tuples are published to {@code topic}.
     * <p>
     * The messages added to Kafka include a topic, message and key.
     * If {@link Message#getKey()} is null, an empty key value is published.
     * 
     * <p>
     * N.B. there seem to be some issues with the underlying 
     * com.ibm.streamsx.messaging library - e.g.,
     * <a href="https://github.com/IBMStreams/streamsx.messaging/issues/118">issue#118</a>.
     * If your application is experiencing odd Kafka behavior
     * try isolating the producer from its feeding streams.
     * e.g.,
     * <pre>
     * ProducerConnector pc = ...
     * TStream<Message> s = ...
     * pc.publish(s.isolate(), ...);
     * </pre> 
     * 
     * @param stream the stream to publish
     * @param topic topic to publish to.  May be null.
     * @return the sink element
     * 
     * @throws IllegalArgumentException if an empty {@code topic} is specified.
     */
    public TSink publish(TStream<? extends Message> stream, String topic) {
        if (topic!=null && topic.isEmpty())
            throw new IllegalArgumentException("topic");
        
        stream = stream.lowLatency();
        
        @SuppressWarnings("unchecked")
        SPLStream splStream = SPLStreams.convertStream((TStream<Message>)stream,
                cvtMsgFunc(topic), KafkaSchemas.KAFKA);
        
        Map<String,Object> params = new HashMap<String,Object>();
        if (!config.isEmpty())
            params.put("kafkaProperty", Util.toKafkaProperty(config));

        // workaround streamsx.messaging issue #107
        params.put("propertiesFile", PROP_FILE_PARAM);
        addPropertiesFile();
       
        // Use SPL.invoke to avoid adding a compile time dependency
        // to com.ibm.streamsx.messaging since JavaPrimitive.invoke*()
        // lack "kind" based variants.
        String kind = "com.ibm.streamsx.messaging.kafka::KafkaProducer";
        String className = "com.ibm.streamsx.messaging.kafka.KafkaSink";
        TSink sink = SPL.invokeSink(
                kind,
                splStream,
                params);
        SPL.tagOpAsJavaPrimitive(sink.operator(), kind, className);
        return sink;
    }
    
    private static BiFunction<Message,OutputTuple,OutputTuple> 
            cvtMsgFunc(final String topic)
    {
        return new BiFunction<Message,OutputTuple,OutputTuple>() {
            private static final long serialVersionUID = 1L;

            @Override
            public OutputTuple apply(Message v1, OutputTuple v2) {
                v2.setString("key", toSplValue(v1.getKey()));
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
