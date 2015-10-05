/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package mqtt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.logic.Value;
import com.ibm.streamsx.topology.messaging.mqtt.MqttStreams;
import com.ibm.streamsx.topology.tuple.Message;
import com.ibm.streamsx.topology.tuple.SimpleMessage;


/**
 * Demonstrate integrating with the MQTT messaging system
 * <a href="http://mqtt.org">http://mqtt.org</a>.
 * <p>
 * {@link com.ibm.streamsx.topology.messaging.mqtt.MqttStreams MqttStreams} is
 * a connector used to create a bridge between topology streams
 * and an MQTT broker.
 * <p>
 * The sample publishes some messages to a MQTT topic.  
 * It also subscribes to the topic and reports the messages received.
 * The messages received may include messages from prior runs of the sample.
 * <p>
 * By default, the sample requires a running MQTT broker with the following
 * characteristics:
 * <ul>
 * <li>the broker's connection is {@code tcp://localhost:1883}</li>
 * <li>the broker is configured for no authentication</li>
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
 * This may be executed from the {@code samples/java/functional} directory as:
 * <UL>
 * <LI>{@code ant run.mqtt.distributed} - Using Apache Ant, this will run in distributed mode.</li>
 * <LI>{@code ant run.mqtt} - Using Apache Ant, this will run in standalone mode.</li>
 * <LI>
 * {@code java -cp functionalsamples.jar:../../../com.ibm.streamsx.topology/lib/com.ibm.streamsx.topology.jar:$STREAMS_INSTALL/lib/com.ibm.streams.operator.samples.jar
 *  mqtt.MqttSample CONTEXT_TYPE
 *     [serverURI=<value>]
 *     [userID=<value>] [password=<value>]
 *     [trustStore=<value>] [trustStorePassword=<value>]
 *     [keyStore=<value>] [keyStorePassword=<value>]
 * } - Run directly from the command line.
 * </LI>
 * Specify absolute pathnames if using the {@code trustStore}
 * or {@code keyStore} arguments.
 * <BR>
 * <i>CONTEXT_TYPE</i> is one of:
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
    private static String SERVER_URI = "tcp://localhost:1883";    
    
    private static final String TOPIC = "mqttSampleTopic";

    private static final int PUB_DELAY_MSEC = 5*1000;
    private boolean captureArtifacts = false;
    private boolean setAppTracingLevel = false;
    private java.util.logging.Level appTracingLevel = java.util.logging.Level.FINE;
    private static final Map<String,String> authInfo = new HashMap<>();
    
    public static void main(String[] args) throws Exception {
        String contextType = "DISTRIBUTED";
        if (args.length > 0)
            contextType = args[0];
        processArgs(args);
        System.out.println("\nUsing MQTT broker at " + SERVER_URI +"\n");

        MqttSample app = new MqttSample();
        app.publishSubscribe(contextType);
    }
    
    /**
     * Publish some messages to a topic; subscribe to the topic and report
     * received messages.
     * @param contextType string value of a {@code StreamsContext.Type}
     * @throws Exception
     */
    public void publishSubscribe(String contextType) throws Exception {
        
        Map<String,Object> contextConfig = new HashMap<>();
        initContextConfig(contextConfig);
        
        Topology top = new Topology("mqttSample");

        // A compile time MQTT topic value.
        Supplier<String> topic = new Value<String>(TOPIC);

        // Create the MQTT connector
        MqttStreams mqtt = new MqttStreams(top, createMqttConfig());
        
        // Create a stream of messages and for the sample, give the
        // consumer a change to become ready
        TStream<Message> msgs = makeStreamToPublish(top)
                        .modify(initialDelayFunc(PUB_DELAY_MSEC));

        // Publish the message stream to the topic
        mqtt.publish(msgs, topic);
        
        // Subscribe to the topic and report received messages
        TStream<Message> rcvdMsgs = mqtt.subscribe(topic);
        rcvdMsgs.print();

        // Submit the topology, to send and receive the messages.
        Future<?> future = StreamsContextFactory.getStreamsContext(contextType)
                .submit(top, contextConfig);
        
        if (contextType.contains("DISTRIBUTED")) {
            System.out.println("\nSee the job's PE console logs for the topology output.\n"
                    + "Use Streams Studio or streamtool.  e.g.,\n"
                    + "    # identify the job's \"Print\" PE\n"
                    + "    streamtool lspes --jobs " + future.get() + "\n"
                    + "    # print the PE's console log\n"
                    + "    streamtool viewlog --print --console --pe <the-peid>"
                    + "\n");
            System.out.println("Cancel the job using Streams Studio or streamtool. e.g.,\n"
                    + "    streamtool canceljob " + future.get()
                    + "\n");
        }
        else if (contextType.contains("STANDALONE")) {
            Thread.sleep(15000);
            future.cancel(true);
        }
    }
    
    private Map<String,Object> createMqttConfig() {
        Map<String,Object> props = new HashMap<>();
        props.put("serverURI", SERVER_URI);
        props.putAll(authInfo);
        return props;
    }
    
    private static void processArgs(String[] args) {
        String item = "serverURI";
        String value = getArg(item, args);
        if (value != null) {
            SERVER_URI = value;
            System.out.println("Using "+item+"="+value);
        }
        initAuthInfo("userID", args);
        initAuthInfo("password", args);
        if (authInfo.containsKey("password") && !authInfo.containsKey("userID"))
            authInfo.put("userID", System.getProperty("user.name"));
        initAuthInfo("trustStore", args);
        initAuthInfo("trustStorePassword", args);
        initAuthInfo("keyStore", args);
        initAuthInfo("keyStorePassword", args);
    }

    private static void initAuthInfo(String item, String[] args) {
        String value = getArg(item, args);
        if (value != null) {
            authInfo.put(item, value);
            if (item.toLowerCase().contains("password"))
                value = "*****";
            System.out.println("Using "+item+"="+value);
        }
    }
    
    private static String getArg(String item, String[] args) {
        for (String arg : args) {
            String[] parts = arg.split("=");
            if (item.equals(parts[0]))
                return parts[1];
        }
        return null;
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
    
    private void initContextConfig(Map<String,Object> contextConfig) {
        if (captureArtifacts)
            contextConfig.put(ContextProperties.KEEP_ARTIFACTS, true);
        if (setAppTracingLevel)
            contextConfig.put(ContextProperties.TRACING_LEVEL, appTracingLevel);
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
