/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.messaging.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static com.ibm.streamsx.topology.messaging.kafka.Util.identifyStreamsxMessagingVer;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.logic.Value;
import com.ibm.streamsx.topology.messaging.kafka.KafkaConsumer;
import com.ibm.streamsx.topology.messaging.kafka.KafkaProducer;
import com.ibm.streamsx.topology.test.InitialDelay;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;
import com.ibm.streamsx.topology.tuple.Message;
import com.ibm.streamsx.topology.tuple.SimpleMessage;


/**
 * N.B. Optional configuration properties:
 * <ul>
 * <li>{@code com.ibm.streamsx.topology.test.messaging.kafka.csvTopics} the
 *     pre-existing kafka topic(s) to use. Defaults to "testTopic1,testTopic2".</li>
 * <li>{@code com.ibm.streamsx.topology.test.messaging.kafka.zookeeper.connect} the
 *      Kafka cluster's zookeeper connection string. Defaults to "localhost:2181".</li>
 * <li>{@code com.ibm.streamsx.topology.test.messaging.kafka.metadata.broker.list} the
 *      Kafka cluster's brokers addresses. Defaults to "localhost:9092".</li>
 * </ul>
 */
public class KafkaStreamsTest extends TestTopology {
    
    /*
     * This testing code is more complicated than simply needed
     * to verify behavior.  It's born out of a need to assist in
     * making the tests resilient and/or debugging failures from
     * potentially (a) the test code, (b) the Java Application API,
     * and (c) the underlying SPL adapter ops.
     * 
     * N.B. Consumer behavior, the ability to receive msgs, is highly
     * dependent on the consumer's Kafka groupId.  i.e., if there's
     * a residual instance of a consumer for the same groupId,
     * or if Kafka/Zookeeper thinks there is, then a new instance
     * of a consumer for the same groupId won't receive any msgs.
     *
     * If these tests are run {@code StreamsContext.Type.STANDALONE},
     * due to com.ibm.streamsx.messaging 
     * <a href="https://github.com/IBMStreams/streamsx.messaging/issues/117">issue#117</a>,
     * such orphaned @{code standalone} processes are created.  Hence
     * the tests generated group ids to avoid being influenced by previous
     * tests.
     * 
     * To see the status of a topic/groupId:
     * <pre>
     * bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zookeeper HOST:PORT --group THE-GROUP
     * </pre>
     * 
     * Kafka generally only delivers msgs received *after* a consumer
     * starts up.  If the producer publishes msgs before the consumer starts,
     * the consumer may not receive the expected msgs.
     * Hence the the tests delay the initial publishing of msgs.
     * 
     * Early on, cases were encountered where a consumer for a test received
     * msgs generated from the previous test (topics are reused and so were the
     * groupIds).
     * Hence the tests work to select only msgs generated for the test.
     */
    
    private static final int SEC_TIMEOUT = 30;
    private static final int PUB_DELAY_MSEC = 5*1000;
    private final String BASE_GROUP_ID = "kafkaStreamsTestGroupId";
    private static final String uniq = simpleTS();
    private boolean captureArtifacts = false;
    private boolean setAppTracingLevel = false;
    private java.util.logging.Level appTracingLevel = java.util.logging.Level.FINE;
    
    private void setupDebug() {
        if (captureArtifacts)
            getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        if (setAppTracingLevel)
            getConfig().put(ContextProperties.TRACING_LEVEL, appTracingLevel);
    }
    
    private static String simpleTS() {
        return new SimpleDateFormat("HH:mm:ss.SSS").format(new Date());
    }
   
    private String newGroupId(String name) {
        String groupId = BASE_GROUP_ID + "_" + name + "_" + uniq.replaceAll(":", "");
// groupId = "FIX_ME2_GROUP_ID";
        System.out.println("["+simpleTS()+"] "
                + "Using Kafka consumer group.id " + groupId);
        return groupId;
    }
    
    private static List<Vals> testMsgVals = new ArrayList<>();
    static {
        testMsgVals.add(new Vals("myMsg", "myKey", "myTopic"));
        testMsgVals.add(new Vals("myMsg", null, "myTopic"));
        testMsgVals.add(new Vals("myMsg", "myKey", null));
        testMsgVals.add(new Vals("myMsg", null, null));
    }
    
    /**
     * Class with no type relationship to Message
     */
    public static class Vals implements Serializable {
        private static final long serialVersionUID = 1L;
        String key;
        String msg;
        String topic;
        Vals(String msg, String key, String topic) {
            this.key = key;
            this.msg = msg;
            this.topic = topic;
        }
        boolean hasTopic() {
            return topic!=null;
        }
        boolean hasKey() {
            return key!=null;
        }
        public String toString() {
            return "msg="+msg+", key="+key+", topic="+topic;
        }
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
        
        MyMsgSubtype(String msg) { this(msg, null, null); }
        MyMsgSubtype(String msg, String key) { this(msg, key, null); }
        MyMsgSubtype(String msg, String key, String topic) {
            this.msg = msg;
            this.key = key;
            this.topic = topic;
        }
        @Override
        public String getMessage() { return msg; }
        @Override
        public String getKey() { return key; }
        @Override
        public String getTopic() { return topic; }
        @Override
        public String toString() { return "["+more+"] "+super.toString(); }
    }
    
    private String[] getKafkaTopics() {
        String csvTopics = System.getProperty("com.ibm.streamsx.topology.test.messaging.kafka.csvTopics", "testTopic1,testTopic2");
        String[] topics = csvTopics.split(",");
        return topics;
    }
    
    private String getKafkaZookeeperConnect() {
        return System.getProperty("com.ibm.streamsx.topology.test.messaging.kafka.zookeeper.connect", "localhost:2181");
    }
    
    private String getKafkaBootstrapServers() {
        return System.getProperty("com.ibm.streamsx.topology.test.messaging.kafka.bootstrap.servers", "localhost:9092");
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
        String create(String topic, String key, String baseContent) {
            return String.format("%s [topic=%s,key=%s] %s", id.next(), topic, key, baseContent);
        }
        String create(String topic, String baseContent) {
            return create(topic, null, baseContent);
        }
        String pattern() {
            return id.pattern();
        }
    }
    
    private Map<String,Object> createConsumerConfig(String groupId) {
        Map<String,Object> props = new HashMap<>();
        props.put("zookeeper.connect", getKafkaZookeeperConnect());
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return props;
    }
    
    private Map<String,Object> createProducerConfig() {
        Map<String,Object> props = new HashMap<>();
        try {
            if (identifyStreamsxMessagingVer().startsWith("2.0")) {
                props.put("metadata.broker.list", getKafkaBootstrapServers());
                props.put("serializer.class", "kafka.serializer.StringEncoder");
                props.put("request.required.acks", "1");
            }
            else {
                // starting with steamsx.messaging v3.0, the 
                // kafka "new producer configs" are used. 
                props.put("bootstrap.servers", getKafkaBootstrapServers());
                props.put("acks", "1");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unable to identify com.ibm.streamsx.messaging toolkit version: " + e);
        }
        return props;
    }
    
    @Test
    public void testSimpleMessage() throws Exception {
        // test ctors
        for (Vals vals : testMsgVals) {
            SimpleMessage t = !vals.hasTopic() ? new SimpleMessage(vals.msg, vals.key)
                            : new SimpleMessage(vals.msg, vals.key, vals.topic);
            assertEquals("ctor "+vals.toString(), vals.key, t.getKey());
            assertEquals("ctor "+vals.toString(), vals.msg, t.getMessage());
            assertEquals("ctor "+vals.toString(), vals.topic, t.getTopic());
            if (!vals.hasKey() && !vals.hasTopic()) {
                t = new SimpleMessage(vals.msg);
                assertEquals("ctor "+vals.toString(), vals.key, t.getKey());
                assertEquals("ctor "+vals.toString(), vals.msg, t.getMessage());
                assertEquals("ctor "+vals.toString(), vals.topic, t.getTopic());
            }
            
            Exception exc = null;
            try {
                new SimpleMessage(null, "key", "topic");  // throw IAE
            } catch (Exception e) {
                exc = e;
            }
            assertNotNull(exc);
            assertTrue(exc.getClass().toString(), exc instanceof IllegalArgumentException);
        }
        // test equals()
        for (Vals vals : testMsgVals) {
            Message t = !vals.hasTopic() ? new SimpleMessage(vals.msg, vals.key)
                            : new SimpleMessage(vals.msg, vals.key, vals.topic);
            Message t2 = !vals.hasTopic() ? new SimpleMessage(vals.msg, vals.key)
                            : new SimpleMessage(vals.msg, vals.key, vals.topic);
            assertTrue("same equals() - "+t.toString(), t.equals(t));
            assertTrue("equals() - "+t.toString(), t.equals(t2));
        }
        // test equals() negative
        for (Vals vals : testMsgVals) {
            Message t = !vals.hasTopic() ? new SimpleMessage(vals.msg, vals.key)
                             : new SimpleMessage(vals.msg, vals.key, vals.topic);
            Message t2 = !vals.hasTopic() ? new SimpleMessage("someOtherValue", "someOtherKey")
                            : new SimpleMessage("someOtherValue", "someOtherKey", "someOtherTopic");
            assertFalse("neg equals() - "+t.toString(), t.equals(t2));

            t2 = new SimpleMessage(vals.msg, vals.key, "someOtherTopic");
            assertFalse("neg equals() - "+t.toString(), t.equals(t2));
            
            t2 = new SimpleMessage("someOtherValue", vals.key);
            assertFalse("neg equals() - "+t.toString(), t.equals(t2));
            
            t2 = new SimpleMessage(vals.msg, "someOtherKey");
            assertFalse("neg equals() - "+t.toString(), t.equals(t2));
            
            assertFalse("neg equals() - "+t.toString(), t.equals(null));
            assertFalse("neg equals() - "+t.toString(), t.equals("notAMessageObject"));
        }
        // test hashCode()
        {
            SimpleMessage t = new SimpleMessage("x", "y", "z");
            assertTrue(t.hashCode() != 0);
            t = new SimpleMessage("x", null, null);
            assertTrue(t.hashCode() != 0);
        }
        
    }
    
    private void checkAssumes() {
        // can we run embedded (all-java ops) if streamsx.messaging jar is in classpath?
        assumeTrue(!isEmbedded());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testSubscribeNullTopic() throws Exception {
        Topology top = new Topology("testSubscribeNullTopic");
        KafkaConsumer consumer = new KafkaConsumer(top, createConsumerConfig("group"));
        
        consumer.subscribe(null); // throws IAE
   }

    @Test (expected = IllegalArgumentException.class)
    public void testSubscribeNullThreadsPerTopic() throws Exception {
        Topology top = new Topology("testSubscribeNullThreadsPerTopic");
        KafkaConsumer consumer = new KafkaConsumer(top, createConsumerConfig("group"));
        
        consumer.subscribe(null, new Value<String>("topic")); // throws IAE
   }

    @Test (expected = IllegalArgumentException.class)
    public void testSubscribeNegThreadsPerTopic() throws Exception {
        Topology top = new Topology("testSubscribeNegThreadsPerTopic");
        KafkaConsumer consumer = new KafkaConsumer(top, createConsumerConfig("group"));
        
        consumer.subscribe(new Value<Integer>(-1), new Value<String>("topic")); // throws IAE
   }
    
    
    @Test
    public void testConfigParams() throws Exception {
        
        Topology top = new Topology("testConfigParams");
        String groupId = newGroupId(top.getName());

        Map<String,Object> producerConfig = createProducerConfig();
        Map<String,Object> consumerConfig = createConsumerConfig(groupId);
        
        KafkaProducer producer = new KafkaProducer(top, producerConfig);
        
        Map<String,Object> pcfg = producer.getConfig();
        for (Object o : producerConfig.keySet())
            assertEquals("property "+o, producerConfig.get(o), pcfg.get(o));
        
        KafkaConsumer consumer = new KafkaConsumer(top, consumerConfig);
        
        Map<String,Object> ccfg = consumer.getConfig();
        for (Object o : consumerConfig.keySet())
            assertEquals("property "+o, consumerConfig.get(o), ccfg.get(o));
    }
    
    @Test
    public void testExplicitTopicProducer() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testNewExplicitTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String groupId = newGroupId(top.getName());

        String topicVal = getKafkaTopics()[0];
        Supplier<String> topic = new Value<String>(topicVal);
        
        KafkaProducer producer = new KafkaProducer(top, createProducerConfig());
        KafkaConsumer consumer = new KafkaConsumer(top, createConsumerConfig(groupId));
        
        // Test producer that takes an arbitrary TStream<T> and explicit topic
        
        List<Vals> msgs = new ArrayList<>();
        msgs.add(new Vals(mgen.create(topicVal, "Hello"), null, null));
        msgs.add(new Vals(mgen.create(topicVal, "key1", "Are you there?"), "key1", null));
        msgs.add(new Vals(mgen.create(topicVal, "Msg with an empty key"), "", null));
        msgs.add(new Vals("", mgen.create(topicVal, null, "Msg with an empty msg (this is the key)"), null));
        
        TStream<Vals> valsToPublish = top.constants(msgs).asType(Vals.class);
        
        TStream<Message> msgsToPublish = valsToPublish.transform(msgFromValsFunc(null));
        
        msgsToPublish = msgsToPublish.modify(new InitialDelay<Message>(PUB_DELAY_MSEC));
        producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        List<Message> expectedAsMessage = mapList(msgs,
                                            msgFromValsFunc(topicVal));
        expectedAsMessage = modifyList(expectedAsMessage, adjustKey());
        List<String> expectedAsString = mapList(expectedAsMessage,
                                            msgToJSONStringFunc());

        setupDebug();
        completeAndValidate(groupId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    @Test
    public void testReusableApp() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testReusableApp");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String groupId = newGroupId(top.getName());

        String topicVal = getKafkaTopics()[0];
        Supplier<String> topic = top.createSubmissionParameter("kafka.topic", topicVal);
        Supplier<Integer> threadsPerTopic = top.createSubmissionParameter("kafka.consumer.threadsPerTopic", 1);
        
        KafkaProducer producer = new KafkaProducer(top, createProducerConfig());
        KafkaConsumer consumer = new KafkaConsumer(top, createConsumerConfig(groupId));
        
        // Test producer that takes an arbitrary TStream<T> and explicit topic
        
        List<Vals> msgs = new ArrayList<>();
        msgs.add(new Vals(mgen.create(topicVal, "Hello"), null, null));
        msgs.add(new Vals(mgen.create(topicVal, "key1", "Are you there?"), "key1", null));
        msgs.add(new Vals(mgen.create(topicVal, "Msg with an empty key"), "", null));
        msgs.add(new Vals("", mgen.create(topicVal, null, "Msg with an empty msg (this is the key)"), null));
        
        TStream<Vals> valsToPublish = top.constants(msgs).asType(Vals.class);
        
        TStream<Message> msgsToPublish = valsToPublish.transform(msgFromValsFunc(null));
        
        msgsToPublish = msgsToPublish.modify(new InitialDelay<Message>(PUB_DELAY_MSEC));
        producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(threadsPerTopic, topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        List<Message> expectedAsMessage = mapList(msgs,
                                            msgFromValsFunc(topicVal));
        expectedAsMessage = modifyList(expectedAsMessage, adjustKey());
        List<String> expectedAsString = mapList(expectedAsMessage,
                                            msgToJSONStringFunc());

        setupDebug();
        completeAndValidate(groupId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    @Test
    public void testImplicitTopicProducer() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testImplicitTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String groupId = newGroupId(top.getName());

        String topicVal = getKafkaTopics()[0];
        Supplier<String> topic = new Value<String>(topicVal);

        KafkaProducer producer = new KafkaProducer(top, createProducerConfig());
        KafkaConsumer consumer = new KafkaConsumer(top, createConsumerConfig(groupId));
        
        // Test producer that takes an arbitrary TStream<T> and implicit topic
        
        List<Vals> msgs = new ArrayList<>();
        msgs.add(new Vals(mgen.create(topicVal, "Hello"), null, null));
        msgs.add(new Vals(mgen.create(topicVal, "key1", "Are you there?"), "key1", null));
        
        TStream<Vals> valsToPublish = top.constants(msgs).asType(Vals.class);
        
        TStream<Message> msgsToPublish = valsToPublish.transform(msgFromValsFunc(topicVal));
        
        msgsToPublish = msgsToPublish.modify(new InitialDelay<Message>(PUB_DELAY_MSEC));
        producer.publish(msgsToPublish);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        List<Message> expectedAsMessage = mapList(msgs,
                                            msgFromValsFunc(topicVal));
        List<String> expectedAsString = mapList(expectedAsMessage,
                                            msgToJSONStringFunc());

        setupDebug();
        completeAndValidate(groupId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    @Test
    public void testMsgImplProducer() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testMsgImplProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String groupId = newGroupId(top.getName());

        String topicVal = getKafkaTopics()[0];
        Supplier<String> topic = new Value<String>(topicVal);

        KafkaProducer producer = new KafkaProducer(top, createProducerConfig());
        KafkaConsumer consumer = new KafkaConsumer(top, createConsumerConfig(groupId));
        
        // Test producer that takes TStream<SimpleMessage> and an explicit topic.
        
        List<Message> msgs = new ArrayList<>();
        msgs.add(new SimpleMessage(mgen.create(topicVal, "Hello")));
        msgs.add(new SimpleMessage(mgen.create(topicVal, "key1", "Are you there?"), "key1"));
        
        TStream<Message> msgsToPublish = top.constants(msgs);
        
        msgsToPublish = msgsToPublish.modify(new InitialDelay<Message>(PUB_DELAY_MSEC));
        producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        msgs = modifyList(msgs, setTopic(topicVal));
        List<String> expectedAsString = mapList(msgs,
                                            msgToJSONStringFunc());

        setupDebug();
        completeAndValidate(groupId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    @Test
    public void testSubtypeExplicitTopicProducer() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testSubtypeExplicitTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String groupId = newGroupId(top.getName());

        String topicVal = getKafkaTopics()[0];
        Supplier<String> topic = new Value<String>(topicVal);

        KafkaProducer producer = new KafkaProducer(top, createProducerConfig());
        KafkaConsumer consumer = new KafkaConsumer(top, createConsumerConfig(groupId));
       
        // Test producer that takes a TStream<MyMsgSubtype>
        
        List<MyMsgSubtype> msgs = new ArrayList<>();
        msgs.add(new MyMsgSubtype(mgen.create(topicVal, "Hello")));
        msgs.add(new MyMsgSubtype(mgen.create(topicVal, "key1", "Are you there?"), "key1"));
        
        TStream<MyMsgSubtype> msgsToPublish = top.constants(msgs).asType(MyMsgSubtype.class);
        
        msgsToPublish = msgsToPublish.modify(new InitialDelay<MyMsgSubtype>(PUB_DELAY_MSEC));
        producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        List<String> expectedAsString = mapList(msgs,
                                            subtypeMsgToJSONStringFunc(topicVal));

        setupDebug();
        completeAndValidate(groupId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    @Test
    public void testSubtypeImplicitTopicProducer() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testSubtypeImplicitTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String groupId = newGroupId(top.getName());

        String topicVal = getKafkaTopics()[0];
        Supplier<String> topic = new Value<String>(topicVal);

        KafkaProducer producer = new KafkaProducer(top, createProducerConfig());
        KafkaConsumer consumer = new KafkaConsumer(top, createConsumerConfig(groupId));
        
        // Test producer that takes a TStream<MyMsgSubtype> implicit topic
        
        List<MyMsgSubtype> msgs = new ArrayList<>();
        msgs.add(new MyMsgSubtype(mgen.create(topicVal, "Hello"), null, topicVal));
        msgs.add(new MyMsgSubtype(mgen.create(topicVal, "key1", "Are you there?"), "key1", topicVal));
        
        TStream<MyMsgSubtype> msgsToPublish = top.constants(msgs).asType(MyMsgSubtype.class);
        
        msgsToPublish = msgsToPublish.modify(new InitialDelay<MyMsgSubtype>(PUB_DELAY_MSEC));
        producer.publish(msgsToPublish);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        List<String> expectedAsString = mapList(msgs,
                                            subtypeMsgToJSONStringFunc(null));

        setupDebug();
        completeAndValidate(groupId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    @Test
    public void testMultiTopicProducer() throws Exception {
        
        checkAssumes();
        
        // streamsx.messaging issue#118 prevents successful execution
        // For standalone it seems to consistently get 0 topic1 msgs.
        assumeTrue(getTesterType() != StreamsContext.Type.STANDALONE_TESTER);
        
        Topology top = new Topology("testMultiTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());
        String groupId = newGroupId(top.getName());

        String[] topics = getKafkaTopics();
        String topic1Val = topics[0];
        String topic2Val = topics[1];
        Supplier<String> topic1 = new Value<String>(topic1Val);
        Supplier<String> topic2 = new Value<String>(topic2Val);


        KafkaProducer producer = new KafkaProducer(top, createProducerConfig());
        KafkaConsumer consumer = new KafkaConsumer(top, createConsumerConfig(groupId));
        
        // Test producer that publishes to multiple topics (implies implicit topic)
        
        List<Message> topic1Msgs = new ArrayList<>();
        topic1Msgs.add(new SimpleMessage(mgen.create(topic1Val, "Hello"), null, topic1Val));
        topic1Msgs.add(new SimpleMessage(mgen.create(topic1Val, "Are you there?"), null, topic1Val));
        
        List<Message> topic2Msgs = new ArrayList<>();
        topic2Msgs.add(new SimpleMessage(mgen.create(topic2Val, "Hello"), null, topic2Val));
        topic2Msgs.add(new SimpleMessage(mgen.create(topic2Val, "Are you there?"), null, topic2Val));
        
        List<Message> msgs = new ArrayList<>(topic1Msgs);
        msgs.addAll(topic2Msgs);
        
        TStream<Message> msgsToPublish = top.constants(msgs);
        
        msgsToPublish = msgsToPublish.modify(new InitialDelay<Message>(PUB_DELAY_MSEC));
        producer.publish(msgsToPublish);
        
        TStream<Message> rcvdTopic1Msgs = consumer.subscribe(topic1);
        TStream<Message> rcvdTopic2Msgs = consumer.subscribe(topic2);
        
        // for validation...
        
        TStream<Message> rcvdMsgs = rcvdTopic1Msgs.union(rcvdTopic2Msgs);
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        List<String> expectedAsString = mapList(msgs,
                                            msgToJSONStringFunc());
                                            
        setupDebug();
        completeAndValidateUnordered(groupId, top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
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
     * Convert a Message with an empty key to have a null key.
     */
    private static UnaryOperator<Message> adjustKey() {
        return new UnaryOperator<Message>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Message apply(Message v) {
                String key = v.getKey(); 
                if (key!=null && key.isEmpty())
                    key = null;
                return new SimpleMessage(v.getMessage(), key, v.getTopic());
            }
        };
    }
    
    /**
     * Function for Vals => Message
     * @param topic if null, Message gets {@code Vals.topic),
     *              otherwise {@code topic}
     * @return Function<Vals,Message>
     */
    private static Function<Vals,Message> msgFromValsFunc(final String topic) {
        return new Function<Vals,Message>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Message apply(Vals v) {
                if (topic!=null)
                    return new SimpleMessage(v.msg, v.key, topic);
                else if (v.topic!=null)
                    return new SimpleMessage(v.msg, v.key, v.topic);
                else
                    return new SimpleMessage(v.msg, v.key);
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
                    v = new MyMsgSubtype(v.msg, v.key, topic);
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
