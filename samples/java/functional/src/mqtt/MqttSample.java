/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package mqtt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.messaging.mqtt.ConsumerConnector;
import com.ibm.streamsx.topology.messaging.mqtt.ProducerConnector;
import com.ibm.streamsx.topology.tuple.Message;
import com.ibm.streamsx.topology.tuple.SimpleMessage;


/**
 * Demonstrate integrating with the MQTT messaging system
 * <a href="http://mqtt.org">http://mqtt.org</a>.
 * <p>
 * Connectors are used to create a bridge between topology streams
 * and an MQTT broker:
 * <ul>
 * <li>{@link com.ibm.streamsx.topology.messaging.mqtt.ConsumerConnector mqtt.ConsumerConnector} - subscribe to MQTT topics and create streams of messages.</li>
 * <li>{@link com.ibm.streamsx.topology.messaging.mqtt.ProducerConnector mqtt.ProducerConnector} - publish streams of messages to MQTT topics.</li>
 * </ul>
 * <p>
 * The sample publishes some messages to a MQTT topic.  
 * It also subscribes to the topic and reports the messages received.
 * The messages received may include messages from prior runs of the sample.
 * <p>
 * The sample requires a running MQTT broker with the following
 * characteristics:
 * <ul>
 * <li>the broker's connection is {@code tcp://localhost:1883}</li>
 * </ul>
 * <p>
 * Required IBM Streams environment variables:
 * <ul>
 * <li>STREAMS_INSTALL - the Streams installation directory</li>
 * <li>STREAMS_DOMAIN_ID - the Streams domain to use for context {@code DISTRIBUTED}
 * <li>STREAMS_INSTANCE_ID - the Streams instance to use for context {@code DISTRIBUTED}
 * </ul>
 * <p>
 * See the MQTT link above for information about setting up a MQTT broker.
 * <p>
 * This may be executed as (from the {@code samples/java/functional directory} )
 * as:
 * <UL>
 * <LI>{@code ant run.mqtt.distributed} - Using Apache Ant, this will run in distributed mode.</li>
 * <LI>{@code ant run.mqtt} - Using Apache Ant, this will run in standalone mode.</li>
 * <LI>
 * {@code java -cp functionalsamples.jar:../../../com.ibm.streamsx.topology/lib/com.ibm.streamsx.topology.jar:$STREAMS_INSTALL/lib/com.ibm.streams.operator.samples.jar mqtt.MqttSample} <i>[CONTEXT_TYPE]</i>
 * - Run directly from the command line with a specific
 * {@code StreamsContext} where <i>CONTEXT_TYPE</i> is one of:
 * </LI>
 * <UL>
 * <LI>{@code DISTRIBUTED} - Run as an IBM Streams distributed application.</LI>
 * <LI>{@code STANDALONE} - Run as an IBM Streams standalone application.</LI>
 * <LI>{@code BUNDLE} - Create an IBM Streams application bundle.</LI>
 * <LI>{@code TOOLKIT} - Create an IBM Streams application toolkit.</LI>
 * </UL>
 * <LI>
 * An application execution within your IDE once you set the class path to include the correct jars.</LI>
 * </UL>

 */
public class MqttSample {
    private static final String SERVER_URI = "tcp://localhost:1883";    
    
    private static final String TOPIC = "mqttSampleTopic";

    private static final int PUB_DELAY_MSEC = 5*1000;
    private static final String uniq = new SimpleDateFormat("HH:mm:ss.SSS").format(new Date());
    private boolean captureArtifacts = false;
    private boolean setAppTracingLevel = false;
    private java.util.logging.Level appTracingLevel = java.util.logging.Level.FINE;
    private Map<String,Object> config = new HashMap<>();
    
    public static void main(String[] args) throws Exception {
        String contextType = "DISTRIBUTED";
        if (args.length > 0)
            contextType = args[0];
        System.out.println("\nREQUIRES:"
                + ", MQTT broker at " + SERVER_URI
                + "\n"
                );

        MqttSample app = new MqttSample();
        app.publishSubscribe(contextType);
    }
    
    /**
     * Publish some messages to a topic, scribe to the topic and report
     * received messages.
     * @param contextType string value of a {@code StreamsContext.Type}
     * @throws Exception
     */
    public void publishSubscribe(String contextType) throws Exception {
        
        setupConfig();
        Topology top = new Topology("mqttSample");
        String pubClientId = newClientId(top.getName() + "_pub");
        String subClientId = newClientId(top.getName() + "_sub");

        ProducerConnector producer = new ProducerConnector(top, createProducerConfig(pubClientId));
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig(subClientId));
        
        TStream<Message> msgs = makeStreamToPublish(top);

        // for the sample, give the consumer a chance to become ready
        msgs = msgs.modify(initialDelayFunc(PUB_DELAY_MSEC));

        producer.publish(msgs, TOPIC);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(TOPIC);

        rcvdMsgs.print();  // show what we received

        // Execute the topology, to send and receive the messages.
        Future<?> future = StreamsContextFactory.getStreamsContext(contextType)
                .submit(top, config);
        
        if (contextType.contains("DISTRIBUTED")) {
            System.out.println("\nSee the job's PE console logs for the topology output.\n");
        }
        else if (contextType.contains("STANDALONE")
                || contextType.contains("EMBEDDED")) {
            Thread.sleep(15000);
            future.cancel(true);
        }
    }
    
    private Properties createConsumerConfig(String clientId) {
        Properties props = new Properties();
        props.put("serverURI", SERVER_URI);
        props.put("clientID", clientId);
        return props;
    }
    
    private Properties createProducerConfig(String clientId) {
        Properties props = new Properties();
        props.put("serverURI", SERVER_URI);
        props.put("clientID", clientId);
        return props;
    }
    
    @SuppressWarnings("serial")
    private static TStream<Message> makeStreamToPublish(Topology top) {
        return top.strings("Hello", "Are you there?",
                           "3 of 5", "4 of 5", "5 of 5"
                ).transform(new Function<String,Message>() {
                    private String timestamp;
                    @Override
                    public Message apply(String v) {
                        if (timestamp == null)
                            timestamp = new SimpleDateFormat("HH:mm:ss.SSS ").format(new Date());
                        return new SimpleMessage(timestamp + v);
                    }
                });
    }
    
    private void setupConfig() {
        if (captureArtifacts)
            config.put(ContextProperties.KEEP_ARTIFACTS, true);
        if (setAppTracingLevel)
            config.put(ContextProperties.TRACING_LEVEL, appTracingLevel);
    }
    
    private String newClientId(String name) {
        String clientId = name + "_" + uniq.replaceAll(":", "");
        System.out.println("Using MQTT clientID " + clientId);
        return clientId;
    }

    @SuppressWarnings("serial")
    private static UnaryOperator<Message> initialDelayFunc(final int delayMsec) {
        return new UnaryOperator<Message>() {
            private int initialDelayMsec = delayMsec;
    
            @Override
            public Message apply(Message v) {
                if (initialDelayMsec != -1) {
                    try {
                        Thread.sleep(initialDelayMsec);
                    } catch (InterruptedException e) {
                        // done delaying
                    }
                    initialDelayMsec = -1;
                }
                return v;
            }
        };
    }
}
