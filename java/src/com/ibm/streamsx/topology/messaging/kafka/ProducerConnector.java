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
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.tuple.Message;

/**
 * A simple connector to an Apache Kafka cluster for producing Kafka messages
 * -- publishing to Kafka topics.
 * <p>
 * A single connector is for a specific Kafka Broker as specified by
 * the KafkaConsumer configuration.
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
 * pc.publish("myTopic", msgsToSend);
 * </pre>
 * 
 * @see <a
 *      href="http://kafka.apache.org/documentation.html">Apache Kafka</a>
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
     * Create a KafkaProducer connector for publishing tuples.
     * <p>
     * See the Apache Kafka documentation for KafkaProducer
     * configuration properties.
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
     * Same as {@code produce(null, stream)}.
     * @param stream the stream to publish
     */
    public void publish(TStream<? extends Message> stream)
    {
        publish(null/*topic*/, stream);
    }

    /**
     * Publish {@code stream} tuples to the Kafka Broker.
     * If {@code topic} is null, each tuples is published to the topic
     * specified by its {@link Message#getTopic()}.
     * Otherwise, all topics are published to {@code topic}.
     * @param topic topic to publish to.  May be null.
     * @param stream the stream to publish
     */
    public void publish(String topic, TStream<? extends Message> stream) {
        @SuppressWarnings("unchecked")
        SPLStream splStream = SPLStreams.convertStream((TStream<Message>)stream,
                cvtMsgFunc(topic), KafkaSchemas.KAFKA);
        
        Map<String,Object> params = new HashMap<String,Object>();
        if (!config.isEmpty())
            params.put("kafkaProperty", Util.toKafkaProperty(config));

        // workaround streamsx.messaging issue #107
        params.put("propertiesFile", PROP_FILE_PARAM);
        addPropertiesFile();
       
        SPL.invokeSink(
                "com.ibm.streamsx.messaging.kafka::KafkaProducer",
                splStream,
                params);
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
