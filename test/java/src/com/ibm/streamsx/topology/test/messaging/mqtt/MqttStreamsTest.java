/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.messaging.mqtt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
import com.ibm.streamsx.topology.messaging.mqtt.MqttStreams;
import com.ibm.streamsx.topology.test.InitialDelay;
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
    private final String BASE_CLIENT_ID = "mqttStreamsTestClientId";
    private static final String uniq = simpleTS();
    private boolean captureArtifacts = false;
    private boolean setAppTracingLevel = false;
    private java.util.logging.Level appTracingLevel = java.util.logging.Level.FINE;
    private static final Map<String,String> authInfo = new HashMap<>();
    static {
        System.setProperty(PROP_PREFIX+"userID", System.getProperty("user.name"));
        System.setProperty(PROP_PREFIX+"password", "myMosquittoPw");
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
    
    private Map<String,Object> createConfig(String clientId) {
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
        // can we run embedded (all-java ops) if streamsx.messaging jar is in classpath?
        assumeTrue(!isEmbedded());
    }
    
    @Test (expected = IllegalArgumentException.class)
    public void testSubscribeNullTopic() throws Exception {
        Topology top = new Topology("testSubscribeNullTopic");
        String subClientId = newSubClientId(top.getName());
        MqttStreams mqtt = new MqttStreams(top, createConfig(subClientId));
        
        mqtt.subscribe(null); // throws IAE
   }
    
    @Test
    public void testConfigParams() throws Exception {
        
        checkAssumes();
        setupDebug();
        Topology top = new Topology("testConfigParams");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String clientId = newPubClientId(top.getName());
        String topicVal = getMqttTopics()[0];
        Supplier<String> topic = new Value<String>(topicVal);
        List<Message> msgs = createMsgs(mgen, null/*topic*/);
        
        // Test more config properties to be sure we don't blow up
        
        Map<String,Object>  config = createConfig(clientId);
        config.put("defaultQOS", 1);
        config.put("keepAliveInterval", 20);
        config.put("commandTimeoutMsec", 30000L);
        config.put("reconnectDelayMsec", 5000L);
        config.put("receiveBufferSize", 10);
        config.put("reconnectionBound", 20);
        config.put("retain", false);
        config.put("userID", System.getProperty("user.name"));
        config.put("password", "foobar");
        config.put("trustStore", "/tmp/no-such-trustStore");
        config.put("trustStorePassword", "woohoo");
        config.put("keyStore", "/tmp/no-such-keyStore");
        config.put("keyStorePassword", "woohoo");
   
        MqttStreams mqtt = new MqttStreams(top, config);
        
        Map<String,Object> pcfg = mqtt.getConfig();
        for (String s : config.keySet())
            assertEquals("property "+s, config.get(s), pcfg.get(s));
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(new InitialDelay<Message>(PUB_DELAY_MSEC));

        TSink sink = mqtt.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = mqtt.subscribe(topic);

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
        
        Map<String,Object> producerConfig = createConfig(pubClientId);
        producerConfig.put("userID", userID);
        producerConfig.put("password", password);
        producerConfig.put("trustStore", trustStore);
        producerConfig.put("trustStorePassword", trustStorePassword);
        producerConfig.put("keyStore", keyStore);
        producerConfig.put("keyStorePassword", keyStorePassword);
        
        Map<String,Object>  consumerConfig = createConfig(subClientId);
        consumerConfig.put("userID", userID);
        consumerConfig.put("password", password);
        consumerConfig.put("trustStore", trustStore);
        consumerConfig.put("trustStorePassword", trustStorePassword);
        consumerConfig.put("keyStore", keyStore);
        consumerConfig.put("keyStorePassword", keyStorePassword);
   
        MqttStreams producer = new MqttStreams(top, producerConfig);
        MqttStreams consumer = new MqttStreams(top, consumerConfig);
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(new InitialDelay<Message>(PUB_DELAY_MSEC));

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
        
        // add the actual param values for our call to submit()
        Map<String,Object> submitParams = new HashMap<>();
        submitParams.put("mqtt.serverURI", "tcp://localhost:1883");
        // submitParams.put("mqtt.userID", System.getProperty("user.name"));
        submitParams.put("mqtt.password", "myMosquittoPw");
        submitParams.put("mqtt.pub.topic", topic);
        submitParams.put("mqtt.sub.topic", topic);
        getConfig().put(ContextProperties.SUBMISSION_PARAMS, submitParams);

        
        // Produce and consume the msgs

        Map<String,Object> pconfig = createConfig(pubClientId);
        addMqttParams(pconfig, false, params);
        Map<String,Object> cconfig = createConfig(subClientId);
        addMqttParams(cconfig, true, params);

        MqttStreams producer = new MqttStreams(top, pconfig);
        MqttStreams consumer = new MqttStreams(top, cconfig);
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(new InitialDelay<Message>(PUB_DELAY_MSEC));
        
        TSink sink = producer.publish(msgsToPublish, params.getString("mqtt.pub.topic"));
        
        TStream<Message> rcvdMsgs = consumer.subscribe(params.getString("mqtt.sub.topic"));

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        msgs = modifyList(msgs, setTopic(topic));
        List<String> expectedAsString = mapList(msgs, msgToJSONStringFunc());
        
        if (testBuildOnly(top))
            return;

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
    public void testExplicitTopicProducerSingleConnector() throws Exception {
        
        checkAssumes();
        setupDebug();
        Topology top = new Topology("testExplicitTopicProducerSingleConnector");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String clientId = newPubClientId(top.getName());
        String topicVal = getMqttTopics()[0];
        Supplier<String> topic = new Value<String>(topicVal);
        List<Message> msgs = createMsgs(mgen, null/*topic*/);
        List<String> expectedAsString = mapList(modifyList(msgs, setTopic(topicVal)),
                                                msgToJSONStringFunc());
        
        // Test producer that takes an explicit topic and implicit config qos
        
        MqttStreams mqtt = new MqttStreams(top, createConfig(clientId));
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(new InitialDelay<Message>(PUB_DELAY_MSEC));

        TSink sink = mqtt.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = mqtt.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        
        if (testBuildOnly(top))
            return;

        completeAndValidate(clientId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
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
        
        MqttStreams producer = new MqttStreams(top, createConfig(pubClientId));
        MqttStreams consumer = new MqttStreams(top, createConfig(subClientId));
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(new InitialDelay<Message>(PUB_DELAY_MSEC));

        TSink sink = producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        
        if (testBuildOnly(top))
            return;

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
        
        MqttStreams producer = new MqttStreams(top, createConfig(pubClientId));
        MqttStreams consumer = new MqttStreams(top, createConfig(subClientId));
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(new InitialDelay<Message>(PUB_DELAY_MSEC));
        
        TSink sink = producer.publish(msgsToPublish);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(new Value<String>(topic));

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        
        if (testBuildOnly(top))
            return;

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

        MqttStreams producer = new MqttStreams(top, createConfig(pubClientId));
        MqttStreams consumer = new MqttStreams(top, createConfig(subClientId));
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(new InitialDelay<Message>(PUB_DELAY_MSEC));
        
        TSink sink = producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        
        if (testBuildOnly(top))
            return;

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

        MqttStreams producer = new MqttStreams(top, createConfig(pubClientId));
        MqttStreams consumer = new MqttStreams(top, createConfig(subClientId));
        
        TStream<MyMsgSubtype> msgsToPublish = top.constants(msgs).asType(MyMsgSubtype.class)
                .modify(new InitialDelay<MyMsgSubtype>(PUB_DELAY_MSEC));
        
        TSink sink = producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        
        if (testBuildOnly(top))
            return;

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

        MqttStreams producer = new MqttStreams(top, createConfig(pubClientId));
        MqttStreams consumer = new MqttStreams(top, createConfig(subClientId));
        
        TStream<MyMsgSubtype> msgsToPublish = top.constants(msgs).asType(MyMsgSubtype.class)
                .modify(new InitialDelay<MyMsgSubtype>(PUB_DELAY_MSEC));
        
        TSink sink = producer.publish(msgsToPublish);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(new Value<String>(topic));

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        
        if (testBuildOnly(top))
            return;

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

        MqttStreams producer = new MqttStreams(top, createConfig(pubClientId));
        MqttStreams consumer = new MqttStreams(top, createConfig(subClientId));
        
        TStream<Message> msgsToPublish = top.constants(msgs)
                .modify(new InitialDelay<Message>(PUB_DELAY_MSEC));
        
        TSink sink = producer.publish(msgsToPublish);
        
        TStream<Message> rcvdTopic1Msgs = consumer.subscribe(new Value<String>(topic1));
        TStream<Message> rcvdTopic2Msgs = consumer.subscribe(new Value<String>(topic2));
        
        // for validation...
        
        TStream<Message> rcvdMsgs = rcvdTopic1Msgs.union(rcvdTopic2Msgs);
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        
        if (testBuildOnly(top))
            return;
                                            
        completeAndValidateUnordered(subClientId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
        assertTrue(sink != null);
    }
    
    @Test
    public void testMultiPubSub() throws Exception {
        
        checkAssumes();
        setupDebug();
        Topology top = new Topology("testMultiPubSub");
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
        
        // Multi pub / sub on a single connector

        MqttStreams producer = new MqttStreams(top, createConfig(pubClientId));
        MqttStreams consumer = new MqttStreams(top, createConfig(subClientId));
        
        TStream<Message> topic1MsgsToPublish = top.constants(topic1Msgs)
                .modify(new InitialDelay<Message>(PUB_DELAY_MSEC));
        TStream<Message> topic2MsgsToPublish = top.constants(topic2Msgs)
                .modify(new InitialDelay<Message>(PUB_DELAY_MSEC));
        
        TSink sink1 = producer.publish(topic1MsgsToPublish, new Value<String>(topic1));
        TSink sink2 = producer.publish(topic2MsgsToPublish, new Value<String>(topic2));
        
        TStream<Message> rcvdTopic1Msgs = consumer.subscribe(new Value<String>(topic1));
        TStream<Message> rcvdTopic2Msgs = consumer.subscribe(new Value<String>(topic2));
        
        // for validation...
        
        TStream<Message> rcvdMsgs = rcvdTopic1Msgs.union(rcvdTopic2Msgs);
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        
        if (testBuildOnly(top))
            return;
                                            
        completeAndValidateUnordered(subClientId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
        assertTrue(sink1 != null);
        assertTrue(sink2 != null);
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

