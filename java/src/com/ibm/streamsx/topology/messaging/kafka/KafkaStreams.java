/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.messaging.kafka;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.tuple.Message;
import com.ibm.streamsx.topology.tuple.SimpleMessage;

/**
 * Stream Source and Sink adapters to Apache Kafka messaging system.
 */
public class KafkaStreams {
    private static final String PROP_FILE_PARAM = "etc/kafkaStreams/emptyProperties";
    
    /**
     * Creates a stream of messages from a Kafka Cluster.
     * This is an adapter to a Kafka KafkaConsumer.
     * <p>
     * N.B., A topology that includes this will not support
     * {@code StreamsContext.EMBEDDED}.
     *  
     * @param te Topology element representing the topology the source stream will be contained in.
     * @param kafkaConsumerConfig see the Kafka documentation.
     *        Minimal configuration typically includes:
     * <ul>
     * <li><code>zookeeper.connect</code></li>
     * <li><code>group.id</code></li>
     * <li><code>zookeeper.session.timeout.ms</code></li>
     * <li><code>zookeeper.sync.time.ms</code></li>
     * <li><code>auto.commit.interval.ms</code></li>
     * </ul>
     * @param threadsPerTopic number of threads to allocate to processing each topic
     * @param topics one or more Kafka topics to subscribe to
     * @return TStream&lt;Message>
     *      The generated tuples do not have a {@code topic}.
     * @throws IllegalArgumentException if topics is null or empty.
     * @throws IllegalArgumentException if threadsPerTopic <= 0.
     */
    public static TStream<Message> consumer(TopologyElement te, Properties kafkaConsumerConfig,
            int threadsPerTopic,
            String... topics) {
        
        if (topics==null || topics.length==0)
            throw new IllegalArgumentException("topics");
        if (threadsPerTopic <= 0)
            throw new IllegalArgumentException("threadsPerTopic");

        Map<String, Object> params = new HashMap<>();
        params.put("topic", topics);
        params.put("threadsPerTopic", threadsPerTopic);
        // workaround streamsx.messaging issue #107
        params.put("propertiesFile", PROP_FILE_PARAM);
        if (kafkaConsumerConfig!=null && !kafkaConsumerConfig.isEmpty())
            params.put("kafkaProperty", toKafkaProperty(kafkaConsumerConfig));
        
        addPropertiesFile(te);

        // see streamsx.messaging issue #109
        boolean confirmedMultiTopicConsumerWorks = false;
        if (topics.length==1 || confirmedMultiTopicConsumerWorks) {
            SPLStream rawKafka = SPL.invokeSource(
                            te,
                            "com.ibm.streamsx.messaging.kafka::KafkaConsumer",
                            params,
                            KafkaSchemas.KAFKA);

            return toMessageStream(rawKafka);
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
            
            return kafka = kafka.union(new HashSet<>(list));
        }
    }
    
    /**
     * Creates a stream of messages from a Kafka Cluster.
     * This is an adapter to a Kafka KafkaConsumer.
     * <p>
     * Same as {@code source(te, topic, 1, kafkaConsumerConfig)}
     * @param te Topology element representing the topology the source stream will be contained in.
     * @param kafkaConsumerConfig see the Kafka documentation.
     *        Minimal configuration typically includes:
     * @param topics one or more Kafka topics to subscribe to
     * @return TStream&lt;Message>
     */
    public static TStream<Message> consumer(TopologyElement te, Properties kafkaConsumerConfig,
            String... topics) {
        return consumer(te, kafkaConsumerConfig, 1, topics);
    }
    
    private static String[] toKafkaProperty(Properties props) {
        List<String> list = new ArrayList<String>();
        for (String key : props.stringPropertyNames()) {
            list.add(key+"="+props.getProperty(key));
        }
        return list.toArray(new String[list.size()]);
    }
    
    private static void addPropertiesFile(TopologyElement te) {
        try {
            File tmpDir = Files.createTempDirectory("kafkaStreams").toFile();
            
            Path p = new File(PROP_FILE_PARAM).toPath();
            String dstDirName = p.getName(0).toString();
            Path pathInDst = p.subpath(1, p.getNameCount());
            if (pathInDst.getNameCount() > 1) {
                File dir = new File(tmpDir, pathInDst.getParent().toString());
                dir.mkdirs();
            }
            new File(tmpDir, pathInDst.toString()).createNewFile();
            File location = new File(tmpDir, pathInDst.getName(0).toString());
            
            te.topology().addFileDependency(dstDirName, 
                    location.getAbsolutePath());
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to create a properties file: " + e, e);
        }
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
                return new SimpleMessage(fromSplValue(tuple.getString("message")),
                                    fromSplValue(tuple.getString("key")));
            }
            
            private String fromSplValue(String s) {
                // SPL doesn't allow null value.  For our use,
                // assume an empty string meant null.
                return (s==null || s.isEmpty()) ? null : s;
            }
        }, Message.class);
    }

    /**
     * Send a stream of {@code Message} tuples to a Kafka Cluster.
     * This is an adapter to a Kafka KafkaProducer.
     * <p>
     * N.B., A topology that includes this will not support
     * {@code StreamsContext.EMBEDDED}.
     * @param te
     * @param kafkaProducerConfig
     * @param topic optional Kafka topic to publish to.
     *          Whens specified, a {@link Message#getTopic()} value is ignored.
     * @param stream a stream of {@link Message} to publish.
     *          When {@code topic} is null, each {@code Message}
     *          <b>must</b> contain a {@code topic}.
     */
    public static void producer(TopologyElement te,
            Properties kafkaProducerConfig,
            final String topic,
            TStream<? extends Message> stream)
    {
        @SuppressWarnings("unchecked")
        SPLStream splStream = SPLStreams.convertStream((TStream<Message>)stream, 
                new BiFunction<Message,OutputTuple,OutputTuple>() {
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
                },
                KafkaSchemas.KAFKA);
            
        Map<String,Object> params = new HashMap<String,Object>();
        // workaround streamsx.messaging issue #107
        params.put("propertiesFile", PROP_FILE_PARAM);
        if (kafkaProducerConfig!=null && !kafkaProducerConfig.isEmpty())
            params.put("kafkaProperty", toKafkaProperty(kafkaProducerConfig));
       
        SPL.invokeSink(
                "com.ibm.streamsx.messaging.kafka::KafkaProducer",
                splStream,
                params);
        
        addPropertiesFile(splStream.topology());
    }
}
