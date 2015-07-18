/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.messaging.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tuple.Message;
import com.ibm.streamsx.topology.tuple.SimpleMessage;

/**
 * A simple connector to an Apache Kafka cluster for consuming Kafka messages
 * -- subscribing to Kafka topics and creating a {@code TStream<Message>}.
 * <p>
 * A single connector is for a specific Kafka cluster as specified in
 * the consumer configuration.
 * <p> 
 * A connector can create any number of consumers in the topology.
 * A consumer can subscribe to one or more topics.
 * <p>
 * Sample use:
 * <pre>
 * Topology top = ...
 * Properties consumerConfig = ...
 * ConsumerConnector cc = new ConsumerConnector(top, consumerConfig);
 * TStream<Message> rcvdMsgs = cc.consumer("myTopic");
 * </pre>
 * 
 * @see <a href="http://kafka.apache.org">http://kafka.apache.org</a>
 * @see <a
 *      href="http://ibmstreams.github.io/streamsx.messaging/">com.ibm.streamsx.messaging</a>
 */
public class ConsumerConnector {
    private static final String PROP_FILE_PARAM = "etc/kafkaStreams/emptyConsumerProperties";
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
     * Create a consumer connector for subscribing to topics.
     * <p>
     * See the Apache Kafka documentation for {@code KafkaConsumer}
     * configuration properties at <a href="http://kafka.apache.org">http://kafka.apache.org</a>.
     * <p>
     * Minimal configuration typically includes:
     * <ul>
     * <li><code>zookeeper.connect</code></li>
     * <li><code>group.id</code></li>
     * <li><code>zookeeper.session.timeout.ms</code></li>
     * <li><code>zookeeper.sync.time.ms</code></li>
     * <li><code>auto.commit.interval.ms</code></li>
     * </ul>
     * @param te {@link TopologyElement} 
     * @param config KafkaConsumer configuration information.
     */
    public ConsumerConnector(TopologyElement te, Properties config) {
        this.te = te;
        this.config = Util.toMap(config);
    }
    
    /**
     * Get the connector's {@code KafkaConsumer} configuration information.
     * @return the unmodifiable configuration 
     */
    public Map<String,String> getConfig() {
        return Collections.unmodifiableMap(config);
    }
    
    /**
     * Subscribe to one or more Kafka Cluster topics and create a stream of
     * messages.
     * <p>
     * Same as {@code consume(topic, 1)}
     * @param topics one or more Kafka topics to subscribe to
     * @return TStream&lt;Message>
     */
    public TStream<Message> subscribe(
            String... topics)
    {
        return subscribe(1, topics);
    }

    /**
     * Subscribe to one or more Kafka Cluster topics and create a stream of
     * messages.
     * <p>
     * N.B., A topology that includes this will not support
     * {@code StreamsContext.EMBEDDED}.
     * @param threadsPerTopic number of threads to allocate to processing each
     *        topic. 
     * @param topics one or more Kafka topics to subscribe to
     * @return TStream&lt;Message>
     *      The generated {@code Message} tuples have a non-null {@code topic}.
     *      The tuple's {@code key} will be null if the Kafka message
     *      lacked a key or it's key was the empty string. 
     * @throws IllegalArgumentException if topics is null or empty, or if
     *         any of the topic values are null or empty.
     * @throws IllegalArgumentException if threadsPerTopic <= 0.
     */
    public TStream<Message> subscribe(int threadsPerTopic, String... topics)
    {
        if (topics==null || topics.length==0)
            throw new IllegalArgumentException("topics");
        for (String topic : topics) {
            if (topic==null || topic.isEmpty())
                throw new IllegalArgumentException("topics");
        }
        if (threadsPerTopic <= 0)
            throw new IllegalArgumentException("threadsPerTopic");

        Map<String, Object> params = new HashMap<>();
        params.put("topic", topics);
        params.put("threadsPerTopic", threadsPerTopic);
        if (!config.isEmpty())
            params.put("kafkaProperty", Util.toKafkaProperty(config));
        
        // workaround streamsx.messaging issue #107
        params.put("propertiesFile", PROP_FILE_PARAM);
        addPropertiesFile();

        // see streamsx.messaging issue #109
        TStream<Message> rcvdMsgs;
        boolean confirmedMultiTopicConsumerWorks = false;
        if (topics.length==1 || confirmedMultiTopicConsumerWorks) {
            SPLStream rawKafka = SPL.invokeSource(
                            te,
                            "com.ibm.streamsx.messaging.kafka::KafkaConsumer",
                            params,
                            KafkaSchemas.KAFKA);

            rcvdMsgs = toMessageStream(rawKafka);
        }
        else {
            // fake it with N kafkaConsumers
            List<TStream<Message>> list = new ArrayList<TStream<Message>>(topics.length);
            for (String topic : topics) {
                Map<String,Object> myParams = new HashMap<>(params);
                myParams.put("topic", topic);

                SPLStream rawKafka = SPL.invokeSource(
                        te,
                        "com.ibm.streamsx.messaging.kafka::KafkaConsumer",
                        myParams,
                        KafkaSchemas.KAFKA);
                
                list.add(toMessageStream(rawKafka));
            }
            TStream<Message> kafka = list.remove(0);
            
            rcvdMsgs = kafka.union(new HashSet<>(list));
        }
        return rcvdMsgs;
    }

    /**
     * Convert an {@link SPLStream} with schema {@link KafkaSchemas.KAFKA}
     * to a TStream&lt;{@link Message}>.
     * The returned stream will contain a {@code Message} tuple for
     * each tuple on {@code stream}.
     * A runtime error will occur if the schema of {@code stream} doesn't
     * have the attributes defined by {@code KafkaSchemas.KAFKA}.
     * @param stream Stream to be converted to a TStream&lt;Message>.
     * @return Stream of {@code Message} tuples from {@code stream}.
     */
    private static TStream<Message> toMessageStream(SPLStream stream) {
        return stream.convert(new Function<Tuple, Message>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Message apply(Tuple tuple) {
                return new SimpleMessage(tuple.getString("message"),
                                    fromSplValue(tuple.getString("key")),
                                    tuple.getString("topic"));
            }
            
            private String fromSplValue(String s) {
                // SPL doesn't allow null value.  For our use,
                // assume an empty string meant null.
                return (s==null || s.isEmpty()) ? null : s;
            }
        }, Message.class);
    }

}
