/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.kafka;

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

/**
 * Stream Source and Sink adapters to Apache Kafka messaging system.
 */
public class KafkaStreams {
    private static final String PROP_FILE = "etc/emptySPLKafkaAdapterProperties";
    
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
     * @return TStream&lt;KafkaTuple>
     *      The generated tuples do not have a {@code topic}.
     * @throws IllegalArgumentException if topics is null or empty.
     * @throws IllegalArgumentException if threadsPerTopic <= 0.
     */
    public static TStream<KafkaMessage> source(TopologyElement te, Properties kafkaConsumerConfig,
            int threadsPerTopic,
            String... topics) {
        
        if (topics==null || topics.length==0)
            throw new IllegalArgumentException("topics");
        if (threadsPerTopic <= 0)
            throw new IllegalArgumentException("threadsPerTopic");

        Map<String, Object> params = new HashMap<>();
        params.put("topic", topics);
        params.put("threadsPerTopic", threadsPerTopic);
        params.put("propertiesFile", PROP_FILE);
        if (kafkaConsumerConfig!=null && !kafkaConsumerConfig.isEmpty())
            params.put("kafkaProperty", toKafkaProperty(kafkaConsumerConfig));
        
        addPropertiesFile(te);

        boolean confirmedMultiTopicConsumerWorks = false; // seems broken
        if (topics.length==1 || confirmedMultiTopicConsumerWorks) {
            SPLStream rawKafka = SPL.invokeSource(
                            te,
                            "com.ibm.streamsx.messaging.kafka::KafkaConsumer",
                            params,
                            KafkaSchemas.KAFKA);

            return toKafkaTupleStream(rawKafka);
        }
        else {
            // fake it with N kafkaConsumers
            List<TStream<KafkaMessage>> list = new ArrayList<TStream<KafkaMessage>>(topics.length);
            for (String topic : topics) {
                Map<String,Object> myParams = new HashMap<>(params);
                myParams.put("topic", topic);

                SPLStream rawKafka = SPL.invokeSource(
                        te,
                        "com.ibm.streamsx.messaging.kafka::KafkaConsumer",
                        myParams,
                        KafkaSchemas.KAFKA);
                
                list.add(toKafkaTupleStream(rawKafka));
            }
            TStream<KafkaMessage> kafka = list.remove(0);
            
            return kafka = kafka.union(new HashSet<>(list));
        }
    }
    
    /**
     * Same as {@code source(te, topic, 1, kafkaConsumerConfig)}
     * @param te Topology element representing the topology the source stream will be contained in.
     * @param kafkaConsumerConfig see the Kafka documentation.
     *        Minimal configuration typically includes:
     * @param topics one or more Kafka topics to subscribe to
     * @return TStream&lt;KafkaTuple>
     */
    public static TStream<KafkaMessage> source(TopologyElement te, Properties kafkaConsumerConfig,
            String... topics) {
        return source(te, kafkaConsumerConfig, 1, topics);
    }
    
    private static String[] toKafkaProperty(Properties props) {
        List<String> list = new ArrayList<String>();
        for (String key : props.stringPropertyNames()) {
            list.add(key+"="+props.getProperty(key));
        }
        return list.toArray(new String[list.size()]);
    }
    
    private static void addPropertiesFile(TopologyElement te) {
        // TODO create tmp empty properties file "etc/emptyKafkaAdapterProperties"
       /*
         File tmpDir = mkTmpDir
         File etcDir = mkDir(tmpDir, "etc");
         File props = mkFile(tmpDir, PROPS_FILE);
         props.createNewFile();
         
        te.addFileDependency(props.getAbsolutePath());
        
        when will it get removed?  if instead Topology makes the copy...???
         */
    }

    /**
     * Convert an {@link SPLStream} with schema {@link KafkaSchemas.KAFKA}
     * to a TStream&lt;{@link KafkaMessageImpl}>.
     * The returned stream will contain a {@code KafkaTuple} tuple for
     * each tuple {@code T} on {@code stream}.
     * A runtime error will occur if the schema of {@code stream} doesn't
     * have the attributes defined by {@code KafkaSchemas.KAFKA}.
     * @param stream Stream to be converted to a TStream&lt;KafkaTuple>.
     * @return Stream of {@code KafkaTuple} tuples from {@code stream}.
     */
    private static TStream<KafkaMessage> toKafkaTupleStream(SPLStream stream) {

        return stream.convert(new Function<Tuple, KafkaMessage>() {
            private static final long serialVersionUID = 1L;

            @Override
            public KafkaMessage apply(Tuple tuple) {
                return new KafkaMessageImpl(fromSplValue(tuple.getString("message")),
                                    fromSplValue(tuple.getString("key")));
            }
            
            private String fromSplValue(String s) {
                // SPL doesn't allow null value.  For our use,
                // assume an empty string meant null.
                return (s==null || s.isEmpty()) ? null : s;
            }
        }, KafkaMessage.class);
    }

    /**
     * Send a stream of messages to a Kafka Cluster.
     * This is an adapter to a Kafka KafkaProducer.
     * <p>
     * N.B., A topology that includes this will not support
     * {@code StreamsContext.EMBEDDED}.
     *  
     * @param kafkaProducerProperties see the Kafka documentation.
     *          Minimal configuration typically includes:
     * <ul>
     * <li><code>metadata.broker.list</code></li>
     * <li><code>serializer.class</code></li>
     * <li><code>request.required.acks</code></li>
     * </ul>
     * @param topic optional Kafka topic to publish to.
     *          Whens specified, a {@link KafkaMessageImpl#getKafkaTopic()} value is ignored.
     *          If null, {@code toKafkaTupleFunc} must generate tuples that have a topic.
     * @param stream the stream to publish to Kafka
     * @param toKafkaMessageFunc function to generate a {@code KafkaTuple} from a tuple {@code T}.
     *          If {@code topic} is null, the generated {@code KafkaTuple} <b>must</b> contain a {@code topic}.
     */
    public static <T> void sink(TopologyElement te,
            Properties kafkaProducerConfig,
            final String topic,
            TStream<T> stream,
            final Function<T,? extends KafkaMessage> toKafkaMessageFunc)
    {
        SPLStream splStream = SPLStreams.convertStream(stream, 
            new BiFunction<T,OutputTuple,OutputTuple>() {
                private static final long serialVersionUID = 1L;

                @Override
                public OutputTuple apply(T v1, OutputTuple v2) {
                    KafkaMessage kafkaTuple = toKafkaMessageFunc.apply(v1);
                    v2.setString("key", toSplValue(kafkaTuple.getKafkaKey()));
                    v2.setString("message", toSplValue(kafkaTuple.getKafkaMessage()));
                    if (topic==null)
                        v2.setString("topic", toSplValue(kafkaTuple.getKafkaTopic()));
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
        params.put("propertiesFile", PROP_FILE);
        if (kafkaProducerConfig!=null && !kafkaProducerConfig.isEmpty())
            params.put("kafkaProperty", toKafkaProperty(kafkaProducerConfig));
       
        SPL.invokeSink(
                "com.ibm.streamsx.messaging.kafka::KafkaProducer",
                splStream,
                params);
        
        addPropertiesFile(splStream.topology());
    }

    /**
     * Send a stream of {@code KafkaTuple} tuples to a Kafka Cluster.
     * This is an adapter to a Kafka KafkaProducer.
     * <p>
     * N.B., A topology that includes this will not support
     * {@code StreamsContext.EMBEDDED}.
     * @param te
     * @param kafkaProducerConfig
     * @param topic optional Kafka topic to publish to.
     *          Whens specified, a {@link KafkaMessageImpl#getKafkaTopic()} value is ignored.
     * @param stream a stream of {@link KafkaMessageImpl} to publish.
     *          When {@code topic} is null, each {@code KafkaTuple}
     *          <b>must</b> contain a {@code topic}.
     */
    public static void sink(TopologyElement te,
            Properties kafkaProducerConfig,
            final String topic,
            TStream<? extends KafkaMessage> stream)
    {
        @SuppressWarnings("unchecked")
        SPLStream splStream = SPLStreams.convertStream((TStream<KafkaMessage>)stream, 
                new BiFunction<KafkaMessage,OutputTuple,OutputTuple>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public OutputTuple apply(KafkaMessage v1, OutputTuple v2) {
                        v2.setString("key", toSplValue(v1.getKafkaKey()));
                        v2.setString("message", toSplValue(v1.getKafkaMessage()));
                        if (topic==null)
                            v2.setString("topic", toSplValue(v1.getKafkaTopic()));
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
        params.put("propertiesFile", PROP_FILE);
        if (kafkaProducerConfig!=null && !kafkaProducerConfig.isEmpty())
            params.put("kafkaProperty", toKafkaProperty(kafkaProducerConfig));
       
        SPL.invokeSink(
                "com.ibm.streamsx.messaging.kafka::KafkaProducer",
                splStream,
                params);
        
        addPropertiesFile(splStream.topology());
    }
}
