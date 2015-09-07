/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.messaging.mqtt;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.logic.Value;
import com.ibm.streamsx.topology.messaging.mqtt.ConsumerConnector;
import com.ibm.streamsx.topology.messaging.mqtt.ProducerConnector;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;
import com.ibm.streamsx.topology.tuple.Message;
import com.ibm.streamsx.topology.tuple.SimpleMessage;


/**
 * MQTT integration tests.
 * <p>
 * N.B. Optional configuration properties:
 * <ul>
 *     topic(s) to use. Defaults to "testTopic1,testTopic2".</li>
 * <li>{@code com.ibm.streamsx.topology.test.messaging.mqtt.serverURI} the
 *      MQTT broker serverURI. Defaults to "tcp:://localhost:1883".</li>
 * <li>{@code com.ibm.streamsx.topology.test.messaging.mqtt.authMode} the
 *      authentication mode: null, "password", "ssl", "sslClientAuth".
 *      Default is null - no authentication.</li>
 * <li>{@code com.ibm.streamsx.topology.test.messaging.mqtt.userID}
 *      userID for authMode=password</li>
 * <li>{@code com.ibm.streamsx.topology.test.messaging.mqtt.password}
 *      password for authMode=password</li>
 * <li>{@code com.ibm.streamsx.topology.test.messaging.mqtt.trustStore}
 *      trustStore pathname for authMode=ssl,sslClientAuth.</li>
 * <li>{@code com.ibm.streamsx.topology.test.messaging.mqtt.trustStorePassword}
 *      trustStore password for authMode=ssl,sslClientAuth.</li>
 * <li>{@code com.ibm.streamsx.topology.test.messaging.mqtt.keyStore}
 *      keyStore pathname for authMode=sslClientAuth.</li>
 * <li>{@code com.ibm.streamsx.topology.test.messaging.mqtt.keyStorePassword}
 *      keyStore password for authMode=sslClientAuth.</li>
 * </ul>
 */
public class MqttStreamsTest extends TestTopology {
    
    private static final String PROP_PREFIX = "com.ibm.streamsx.topology.test.messaging.mqtt.";
    private static final int SEC_TIMEOUT = 30;
    private static final int PUB_DELAY_MSEC = 5*1000;
    private final String BASE_CLIENT_ID = "mqttStreamsTestCientId";
    private static final String uniq = simpleTS();
    private boolean captureArtifacts = false;
    private boolean setAppTracingLevel = false;
    private java.util.logging.Level appTracingLevel = java.util.logging.Level.FINE;
    private static final Map<String,String> authInfo = new HashMap<>();
    static {
        initAuthInfo("userID");
        initAuthInfo("password");
        initAuthInfo("trustStore");
        initAuthInfo("trustStorePassword");
        initAuthInfo("keyStore");
        initAuthInfo("keyStorePassword");
    }
    
    private void setupDebug() {
        if (captureArtifacts)
            getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        if (setAppTracingLevel)
            getConfig().put(ContextProperties.TRACING_LEVEL, appTracingLevel);
    }
    
    private static String simpleTS() {
        return new SimpleDateFormat("HH:mm:ss.SSS").format(new Date());
    }
    
    private String newSubClientId(String name) {
        return newClientId(name+"_Sub");
    }
    
    private String newPubClientId(String name) {
        return newClientId(name+"_Pub");
    }
   
    private String newClientId(String name) {
        String clientId = BASE_CLIENT_ID + "_" + name + "_" + uniq.replaceAll(":", "");
        System.out.println("["+simpleTS()+"] "
                + "Using MQTT clientID " + clientId);
        return clientId;
    }
    
    /**
     * Class that implements Message
     */
    private static class MyMsgSubtype implements Message {
        private static final long serialVersionUID = 1L;
        private String more = "I am a MessageSubtype";
        private String msg;
        private String key;
        private String topic;
        
        MyMsgSubtype(String msg) { this(msg, null); }
        MyMsgSubtype(String msg, String topic) {
            this.msg = msg;
            this.topic = topic;
        }
        @Override
        public String getMessage() { return msg; }
        @Override
        public String getKey() { return key; }
        @Override
        public String getTopic() { return topic; }
        @Override
        public String toString() { return String.format("{[%s] topic=%s key=%s message=%s", more, topic, key, msg); }
    }
    
    private String[] getMqttTopics() {
        String csvTopics = System.getProperty("com.ibm.streamsx.topology.test.messaging.mqtt.csvTopics", "testTopic1,testTopic2");
        String[] topics = csvTopics.split(",");
        return topics;
    }
    
    private String getServerURI() {
        return System.getProperty("com.ibm.streamsx.topology.test.messaging.mqtt.serverURI", "tcp://localhost:1883");
    }
   
    private static class MsgId {
        private int seq;
        private String uniq;
        private String prefix;
        MsgId(String prefix) {
            this.prefix = prefix;
        }
        String next() {
            if (uniq==null) {
                uniq = new SimpleDateFormat("HH:mm:ss.SSS").format(new Date());
            }
            return String.format("[%s.%d %s]", uniq, seq++, prefix);
        }
        String pattern() {
            return String.format(".*\\[%s\\.\\d+ %s\\].*", uniq, prefix);
        }
    }
    
    private static class MsgGenerator {
        private MsgId id;
        MsgGenerator(String testName) {
            id = new MsgId(testName);
        }
        String create(String topic, String baseContent) {
            return String.format("%s [topic=%s] %s", id.next(), topic, baseContent);
        }
        String pattern() {
            return id.pattern();
        }
    }
    
    private Map<String,Object> createConsumerConfig(String clientId) {
        Map<String,Object> props = new HashMap<>();
        props.put("serverURI", getServerURI());
        props.put("clientID", clientId);
        props.putAll(authInfo);
        return props;
    }
    
    private Map<String,Object> createProducerConfig(String clientId) {
        Map<String,Object> props = new HashMap<>();
        props.put("serverURI", getServerURI());
        props.put("clientID", clientId);
        props.putAll(authInfo);
        return props;
    }
    
    private static void initAuthInfo(String item) {
        String val = System.getProperty(PROP_PREFIX + item);
        if (val != null) {
            authInfo.put(item, val);
            if (item.toLowerCase().contains("password"))
                val = "*****";
            System.out.println("Using "+item+"="+val);
        }
    }

    private void checkAssumes() {
        // Embedded and MQTTConnector implementation leveraging 
        // SPL MQTTSink,Source ops don't mix.
        assumeTrue(!isEmbedded());
        
        assumeTrue(SC_OK);
    }
    
    @Test
    public void testReusableApp() throws Exception {
        
        checkAssumes();
        setupDebug();
        Topology top = new Topology("testReusableApp");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String subClientId = newSubClientId(top.getName());
        String pubClientId = newPubClientId(top.getName());
        String topic = getMqttTopics()[0];
        List<Message> msgs = createMsgs(mgen, null/*topic*/);
        
        // Test an app structured more as a "reusable asset" - i.e.,
        // where the mqtt connection info (URI, authInfo) and
        // topic are defined at submission time.
        
        // define/create the app's submission parameters
        ParameterHelper params = new ParameterHelper(top);
        params.definitions().put("mqtt.serverURI", String.class);
        params.definitions().put("mqtt.userID", System.getProperty("user.name"));
        params.definitions().put("mqtt.password", String.class);
        params.definitions().put("mqtt.pub.topic", String.class);
        params.definitions().put("mqtt.sub.topic", String.class);
        params.createAll();
        
        // add the values for our call to submit()
        Map<String,Object> submitParams = new HashMap<>();
        submitParams.put("mqtt.serverURI", "tcp://localhost:1883");
        // submitParams.put("mqtt.userID", System.getProperty("user.name"));
        submitParams.put("mqtt.password", "myMosquittoPw");
        submitParams.put("mqtt.pub.topic", topic);
        submitParams.put("mqtt.sub.topic", topic);
        getConfig().put(ContextProperties.SUBMISSION_PARAMS, submitParams);

        
        // Produce and consume the msgs

        Map<String,Object> pconfig = createProducerConfig(pubClientId);
        addMqttParams(pconfig, false, params);
        Map<String,Object> cconfig = createConsumerConfig(subClientId);
        addMqttParams(cconfig, true, params);

        ProducerConnector producer = new ProducerConnector(top, pconfig);
        ConsumerConnector consumer = new ConsumerConnector(top, cconfig);
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(initialDelayFunc(PUB_DELAY_MSEC));
        
        TSink sink = producer.publish(msgsToPublish, params.getString("mqtt.pub.topic"));
        
        TStream<Message> rcvdMsgs = consumer.subscribe(params.getString("mqtt.sub.topic"));

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        msgs = modifyList(msgs, setTopic(topic));
        List<String> expectedAsString = mapList(msgs, msgToJSONStringFunc());

        completeAndValidate(subClientId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
        assertTrue(sink != null);
    }
    
    private static void addMqttParams(Map<String,Object> config,
            boolean isConsumer, ParameterHelper params) {
        for (Map.Entry<String, Supplier<?>> e : params.parameters().entrySet()) {
            String name = e.getKey();
            String prefix = "mqtt.";
            if (!name.startsWith(prefix))
                continue;
            name = name.substring(prefix.length());
            
            prefix = isConsumer ? "sub." : "pub.";
            if (name.startsWith("sub.") || name.startsWith("pub.")) {
                if (name.equals(prefix + "topic"))
                    continue;  // not a config param
                if (name.startsWith("sub.") && !prefix.equals("sub.") 
                    || name.startsWith("pub.") && !prefix.equals("pub."))
                    continue;
                name = name.substring(prefix.length());
            }
            
            config.put(name, e.getValue());
        }
    }
    
    @Test
    public void testConfigParams() throws Exception {
        
        checkAssumes();
        setupDebug();
        Topology top = new Topology("testConfigParams");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String subClientId = newSubClientId(top.getName());
        String pubClientId = newPubClientId(top.getName());
        String topicVal = getMqttTopics()[0];
        Supplier<String> topic = new Value<String>(topicVal);
        List<Message> msgs = createMsgs(mgen, null/*topic*/);
        
        // Test more config properties to be sure we don't blow up
        
        Map<String,Object> producerConfig = createProducerConfig(pubClientId);
        producerConfig.put("defaultQOS", 1);
        producerConfig.put("keepAliveInterval", 10);
        producerConfig.put("commandTimeoutMsec", 20000L);
        producerConfig.put("reconnectDelayMsec", 4000L);
        producerConfig.put("reconnectionBound", 10);
        producerConfig.put("retain", false);
        producerConfig.put("userID", System.getProperty("user.name"));
        producerConfig.put("password", "foobar");
        producerConfig.put("trustStore", "/tmp/no-such-trustStore");
        producerConfig.put("trustStorePassword", "woohoo");
        producerConfig.put("keyStore", "/tmp/no-such-keyStore");
        producerConfig.put("keyStorePassword", "woohoo");
        
        Map<String,Object>  consumerConfig = createConsumerConfig(subClientId);
        consumerConfig.put("defaultQOS", 1);
        consumerConfig.put("keepAliveInterval", 20);
        consumerConfig.put("commandTimeoutMsec", 30000L);
        consumerConfig.put("reconnectDelayMsec", 5000L);
        consumerConfig.put("receiveBufferSize", 10);
        consumerConfig.put("reconnectionBound", 20);
        consumerConfig.put("userID", System.getProperty("user.name"));
        consumerConfig.put("password", "foobar");
        consumerConfig.put("trustStore", "/tmp/no-such-trustStore");
        consumerConfig.put("trustStorePassword", "woohoo");
        consumerConfig.put("keyStore", "/tmp/no-such-keyStore");
        consumerConfig.put("keyStorePassword", "woohoo");
   
        ProducerConnector producer = new ProducerConnector(top, producerConfig);
        ConsumerConnector consumer = new ConsumerConnector(top, consumerConfig);
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(initialDelayFunc(PUB_DELAY_MSEC));

        TSink sink = producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();

        // bundle construction fails for unrecognized or incorrectly typed SPL op params
        File actBundle = (File) StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.BUNDLE)
                .submit(top, getConfig())
                .get(15, TimeUnit.SECONDS);
        System.out.println("bundle " + actBundle.getAbsolutePath());
        assertTrue(actBundle != null);
        actBundle.delete();
        assertTrue(sink != null);
    }
    
    @Test
    public void testConfigParamsSubmissionParam() throws Exception {
        
        checkAssumes();
        setupDebug();
        Topology top = new Topology("testConfigParamsSubmissionParam");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String subClientId = newSubClientId(top.getName());
        String pubClientId = newPubClientId(top.getName());
        String topicVal = getMqttTopics()[0];
        Supplier<String> topic = new Value<String>(topicVal);
        List<Message> msgs = createMsgs(mgen, null/*topic*/);
        
        // Test more config properties to be sure we don't blow up
        
        Supplier<String> userID = top.createSubmissionParameter("userID", String.class);
        Supplier<String> password = top.createSubmissionParameter("password", String.class);
        Supplier<String> trustStore = top.createSubmissionParameter("trustStore", String.class);
        Supplier<String> trustStorePassword = top.createSubmissionParameter("trustStorePassword", String.class);
        Supplier<String> keyStore = top.createSubmissionParameter("keyStore", String.class);
        Supplier<String> keyStorePassword = top.createSubmissionParameter("keyStorePassword", String.class);
        
        Map<String,Object> producerConfig = createProducerConfig(pubClientId);
        producerConfig.put("userID", userID);
        producerConfig.put("password", password);
        producerConfig.put("trustStore", trustStore);
        producerConfig.put("trustStorePassword", trustStorePassword);
        producerConfig.put("keyStore", keyStore);
        producerConfig.put("keyStorePassword", keyStorePassword);
        
        Map<String,Object>  consumerConfig = createConsumerConfig(subClientId);
        consumerConfig.put("userID", userID);
        consumerConfig.put("password", password);
        consumerConfig.put("trustStore", trustStore);
        consumerConfig.put("trustStorePassword", trustStorePassword);
        consumerConfig.put("keyStore", keyStore);
        consumerConfig.put("keyStorePassword", keyStorePassword);
   
        ProducerConnector producer = new ProducerConnector(top, producerConfig);
        ConsumerConnector consumer = new ConsumerConnector(top, consumerConfig);
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(initialDelayFunc(PUB_DELAY_MSEC));

        TSink sink = producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();

        // bundle construction fails for unrecognized or incorrectly typed SPL op params
        File actBundle = (File) StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.BUNDLE)
                .submit(top, getConfig())
                .get(15, TimeUnit.SECONDS);
        System.out.println("bundle " + actBundle.getAbsolutePath());
        assertTrue(actBundle != null);
        actBundle.delete();
        assertTrue(sink != null);
    }
    
    @Test
    public void testExplicitTopicProducer() throws Exception {
        
        checkAssumes();
        setupDebug();
        Topology top = new Topology("testNewExplicitTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String subClientId = newSubClientId(top.getName());
        String pubClientId = newPubClientId(top.getName());
        String topicVal = getMqttTopics()[0];
        Supplier<String> topic = new Value<String>(topicVal);
        List<Message> msgs = createMsgs(mgen, null/*topic*/);
        List<String> expectedAsString = mapList(modifyList(msgs, setTopic(topicVal)),
                                                msgToJSONStringFunc());
        
        // Test producer that takes an explicit topic and implicit config qos
        
        ProducerConnector producer = new ProducerConnector(top, createProducerConfig(pubClientId));
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig(subClientId));
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(initialDelayFunc(PUB_DELAY_MSEC));

        TSink sink = producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());

        completeAndValidate(subClientId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
        assertTrue(sink != null);
    }
    
    @Test
    public void testImplicitTopicProducer() throws Exception {
        
        checkAssumes();
        setupDebug();
        Topology top = new Topology("testImplicitTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String subClientId = newSubClientId(top.getName());
        String pubClientId = newPubClientId(top.getName());
        String topic = getMqttTopics()[0];
        List<Message> msgs = createMsgs(mgen, topic);
        List<String> expectedAsString = mapList(msgs, msgToJSONStringFunc());
        
        // Test producer that takes an arbitrary TStream<T> and implicit topic
        
        ProducerConnector producer = new ProducerConnector(top, createProducerConfig(pubClientId));
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig(subClientId));
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(initialDelayFunc(PUB_DELAY_MSEC));
        
        TSink sink = producer.publish(msgsToPublish);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(new Value<String>(topic));

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());

        completeAndValidate(subClientId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
        assertTrue(sink != null);
    }
    
    @Test
    public void testMsgImplProducer() throws Exception {
        
        checkAssumes();
        setupDebug();
        Topology top = new Topology("testMsgImplProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String subClientId = newSubClientId(top.getName());
        String pubClientId = newPubClientId(top.getName());
        String topicVal = getMqttTopics()[0];
        Supplier<String> topic = new Value<String>(topicVal);
        List<Message> msgs = createMsgs(mgen, null/*topic*/);
        List<String> expectedAsString = mapList(modifyList(msgs, setTopic(topicVal)),
                                            msgToJSONStringFunc());
        
        // Test producer that takes TStream<SimpleMessage> and an explicit topic.

        ProducerConnector producer = new ProducerConnector(top, createProducerConfig(pubClientId));
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig(subClientId));
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(initialDelayFunc(PUB_DELAY_MSEC));
        
        TSink sink = producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());

        completeAndValidate(subClientId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
        assertTrue(sink != null);
    }
    
    @Test
    public void testSubtypeExplicitTopicProducer() throws Exception {
        
        checkAssumes();
        setupDebug();
        Topology top = new Topology("testSubtypeExplicitTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String subClientId = newSubClientId(top.getName());
        String pubClientId = newPubClientId(top.getName());
        String topicVal = getMqttTopics()[0];
        Supplier<String> topic = new Value<String>(topicVal);
        List<MyMsgSubtype> msgs = new ArrayList<>();
        for(Message m : createMsgs(mgen, null/*topic*/)) {
            msgs.add(new MyMsgSubtype(m.getMessage()));
        }
        List<String> expectedAsString = mapList(msgs, subtypeMsgToJSONStringFunc(topicVal));
        
        // Test producer that takes a TStream<MyMsgSubtype>

        ProducerConnector producer = new ProducerConnector(top, createProducerConfig(pubClientId));
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig(subClientId));
        
        TStream<MyMsgSubtype> msgsToPublish = top.constants(msgs).asType(MyMsgSubtype.class)
                .modify(myMsgInitialDelayFunc(PUB_DELAY_MSEC));
        
        TSink sink = producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());

        completeAndValidate(subClientId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
        assertTrue(sink != null);
    }
    
    @Test
    public void testSubtypeImplicitTopicProducer() throws Exception {
        
        checkAssumes();
        setupDebug();
        Topology top = new Topology("testSubtypeImplicitTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String subClientId = newSubClientId(top.getName());
        String pubClientId = newPubClientId(top.getName());
        String topic = getMqttTopics()[0];
        List<MyMsgSubtype> msgs = new ArrayList<>();
        for(Message m : createMsgs(mgen, topic)) {
            msgs.add(new MyMsgSubtype(m.getMessage(), m.getTopic()));
        }
        List<String> expectedAsString = mapList(msgs, subtypeMsgToJSONStringFunc(null));

        // Test producer that takes a TStream<MyMsgSubtype> implicit topic

        ProducerConnector producer = new ProducerConnector(top, createProducerConfig(pubClientId));
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig(subClientId));
        
        TStream<MyMsgSubtype> msgsToPublish = top.constants(msgs).asType(MyMsgSubtype.class)
                .modify(myMsgInitialDelayFunc(PUB_DELAY_MSEC));
        
        TSink sink = producer.publish(msgsToPublish);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(new Value<String>(topic));

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());

        completeAndValidate(subClientId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
        assertTrue(sink != null);
    }
    
    @Test
    public void testMultiTopicProducer() throws Exception {
        
        checkAssumes();
        setupDebug();
        Topology top = new Topology("testMultiTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String subClientId = newSubClientId(top.getName());
        String pubClientId = newPubClientId(top.getName());
        String[] topics = getMqttTopics();
        String topic1 = topics[0];
        String topic2 = topics[1];
        List<Message> topic1Msgs = createMsgs(mgen, topic1);
        List<Message> topic2Msgs = createMsgs(mgen, topic2);
        List<Message> msgs = new ArrayList<>(topic1Msgs);
        msgs.addAll(topic2Msgs);
        List<String> expectedAsString = mapList(msgs, msgToJSONStringFunc());
        
        // Test producer that publishes to multiple topics (implies implicit topic)

        ProducerConnector producer = new ProducerConnector(top, createProducerConfig(pubClientId));
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig(subClientId));
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(initialDelayFunc(PUB_DELAY_MSEC));
        
        TSink sink = producer.publish(msgsToPublish);
        
        TStream<Message> rcvdTopic1Msgs = consumer.subscribe(new Value<String>(topic1));
        TStream<Message> rcvdTopic2Msgs = consumer.subscribe(new Value<String>(topic2));
        
        // for validation...
        
        TStream<Message> rcvdMsgs = rcvdTopic1Msgs.union(rcvdTopic2Msgs);
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
                                            
        completeAndValidateUnordered(subClientId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
        assertTrue(sink != null);
    }
    
    // would be nice if Tester provided this too
    private void completeAndValidateUnordered(String msg, Topology topology,
            TStream<String> stream, int secTimeout, String... expected)
            throws Exception {
        
        Tester tester = topology.getTester();

        Condition<Long> sCount = tester.tupleCount(stream, expected.length);
        Condition<List<String>> sContents = tester.stringContentsUnordered(stream, expected);

        complete(tester, sCount, secTimeout, TimeUnit.SECONDS);

        assertTrue(msg + " contents:" + sContents, sContents.valid());
        assertTrue("valid:" + sCount, sCount.valid());
    }
    
    private void completeAndValidate(String msg, Topology topology,
            TStream<String> stream, int secTimeout, String... expected)
            throws Exception {
        
        Tester tester = topology.getTester();

        Condition<Long> sCount = tester.tupleCount(stream, expected.length);
        Condition<List<String>> sContents = tester.stringContents(stream, expected);

        complete(tester, sCount, secTimeout, TimeUnit.SECONDS);

        assertTrue(msg + " contents:" + sContents, sContents.valid());
        assertTrue("valid:" + sCount, sCount.valid());
    }
    
    private List<Message> createMsgs(MsgGenerator mgen, String topic) {
        List<Message> msgs = new ArrayList<>();
        msgs.add(new SimpleMessage(mgen.create(topic, "Hello"), null, topic));
        msgs.add(new SimpleMessage(mgen.create(topic, "Are you there?"), null, topic));
        return msgs;
    }

    private static TStream<Message> selectMsgs(TStream<Message> stream,
                                            final String pattern) { 
        return stream.filter(
            new Predicate<Message>() {
                private static final long serialVersionUID = 1L;
    
                @Override
                public boolean test(Message tuple) {
                    return tuple.getMessage().matches(pattern)
                            || (tuple.getKey()!=null
                                && tuple.getKey().matches(pattern));
                }
            });
    }

    private static UnaryOperator<Message> initialDelayFunc(final int delayMsec) {
        return new UnaryOperator<Message>() {
            private static final long serialVersionUID = 1L;
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

    private static UnaryOperator<MyMsgSubtype> myMsgInitialDelayFunc(final int delayMsec) {
        return new UnaryOperator<MyMsgSubtype>() {
            private static final long serialVersionUID = 1L;
            private int initialDelayMsec = delayMsec;
    
            @Override
            public MyMsgSubtype apply(MyMsgSubtype v) {
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

    /**
     * Modify List<T> => List<T> via func
     */
    private <T> List<T> modifyList(List<T> list, UnaryOperator<T> func) {
        List<T> l = new ArrayList<>();
        for (T o : list) {
            l.add(func.apply(o));
        }
        return l;
    }

    /**
     * Transform List<T> => List<R> via func
     */
    private <T,R> List<R> mapList(List<T> list, Function<T, R> func) {
        List<R> l = new ArrayList<>();
        for (T o : list) {
            l.add(func.apply(o));
        }
        return l;
    }
    
    private static UnaryOperator<Message> setTopic(final String topic) {
        return new UnaryOperator<Message>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Message apply(Message v) {
                return new SimpleMessage(v.getMessage(), v.getKey(), topic);
            }
        };
    }

    /**
     * Function for Message => toJSON().toString()
     */
    private static Function<Message,String> msgToJSONStringFunc() {
        return new Function<Message,String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String apply(Message v) {
                return toJson(v);
            }
        };
    }

    /**
     * Function for MyMsgSubtype => toJSON().toString()
     */
    private static Function<MyMsgSubtype,String> subtypeMsgToJSONStringFunc(final String topic) {
        return new Function<MyMsgSubtype,String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String apply(MyMsgSubtype v) {
                if (topic!=null)
                    v = new MyMsgSubtype(v.msg, topic);
                return toJson(v);
            }
        };
    }
    
    private static String toJson(Message m) {
        JSONObject jo = new JSONObject();
        jo.put("key", m.getKey());
        jo.put("message", m.getMessage());
        jo.put("topic", m.getTopic());
        return jo.toString();

    }
}

/*
 * Use top.supplier(new DelayedSupplierIterable<>(msgs, delay)) 
 * subsequently yields:
 * java.lang.NullPointerException
at com.ibm.streamsx.topology.spl.SPLStreams.convertStream(SPLStreams.java:104)
at com.ibm.streamsx.topology.messaging.mqtt.ProducerConnector.publish(ProducerConnector.java:142)
at com.ibm.streamsx.topology.messaging.mqtt.ProducerConnector.publish(ProducerConnector.java:114)
at com.ibm.streamsx.topology.test.messaging.mqtt.MqttStreamsTest.testConfigParams(MqttStreamsTest.java:288)

 * Looks like the TStream returned by top.constants ends up
 * with its getTupleClass() returning null?
 * [line 104] opName = "SPLConvert" + stream.getTupleClass().getSimpleName();
 */
class DelayedSupplierIterable<T> implements Supplier<Iterable<T>>, Iterable<T>, Iterator<T>, Serializable {
    private static final long serialVersionUID = 1L;
    private final Iterable<T> data;
    private Iterator<T> iter;
    private int initialDelayMsec;
    DelayedSupplierIterable(Iterable<T> data, int initialDelayMsec) {
        this.data = data;
        this.initialDelayMsec = initialDelayMsec;
    }
    @Override
    public Iterable<T> get() {
        return this;
    }
    @Override
    public Iterator<T> iterator() {
        return this;
    }
    @Override
    public boolean hasNext() {
        if (iter==null)
            iter = data.iterator();
        return iter.hasNext();
    }
    @Override
    public T next() {
        if (iter==null)
            iter = data.iterator();
        if (initialDelayMsec != -1) {
            try {
                Thread.sleep(initialDelayMsec);
            } catch (InterruptedException e) {
                // done delaying
            }
            initialDelayMsec = -1;
        }
        return iter.next();
    }
    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}

