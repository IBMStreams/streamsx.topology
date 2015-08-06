/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.messaging.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.messaging.kafka.ConsumerConnector;
import com.ibm.streamsx.topology.messaging.kafka.ProducerConnector;
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
    
    private static int SEC_TIMEOUT = 15;
    
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
    
    private String getKafkaMetadataBrokerList() {
        return System.getProperty("com.ibm.streamsx.topology.test.messaging.kafka.metedata.broker.list", "localhost:9092");
    }
    
    private static class MsgId {
        private int seq;
        private long uniq;
        private String prefix;
        MsgId(String prefix) {
            this.prefix = prefix;
        }
        String next() {
            if (uniq==0)
                uniq = System.currentTimeMillis() % 1000;
            return String.format("[%d.%d %s]", uniq, seq++, prefix);
        }
        String pattern() {
            return String.format(".*\\[%d\\.\\d+ %s\\].*", uniq, prefix);
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
    
    private Properties createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", getKafkaZookeeperConnect());
        props.put("group.id", "myKafkaStreamsTestGroup");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return props;
    }
    
    private Properties createProducerConfig() {
        Properties props = new Properties();
        props.put("metadata.broker.list", getKafkaMetadataBrokerList());
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
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
    }
    
    private void checkAssumes() {
        // Embedded and KafkaConnector implementation leveraging 
        // SPL kafkaProducer,Consumer ops don't mix.
        assumeTrue(!isEmbedded());
        
        // Standalone doesn't work at least with the current test harness.
        // submit()/sc fails due to incompatible VmArgs in a single PE
        assumeTrue(getTesterType() != Type.STANDALONE_TESTER);
        
        assumeTrue(SC_OK);
    }
    
    @Test
    public void testExplicitTopicProducer() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testNewExplicitTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String topic = getKafkaTopics()[0];
        
        ProducerConnector producer = new ProducerConnector(top, createProducerConfig());
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig());
        
        // Test producer that takes an arbitrary TStream<T> and explicit topic
        
        List<Vals> msgs = new ArrayList<>();
        msgs.add(new Vals(mgen.create(topic, "Hello"), null, null));
        msgs.add(new Vals(mgen.create(topic, "key1", "Are you there?"), "key1", null));
        msgs.add(new Vals(mgen.create(topic, "Msg with an empty key"), "", null));
        msgs.add(new Vals("", mgen.create(topic, null, "Msg with an empty msg (this is the key)"), null));
        
        TStream<Vals> valsToPublish = top.constants(msgs);
        
        TStream<Message> msgsToPublish = valsToPublish.transform(msgFromValsFunc(null));
        
        producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        List<Message> expectedAsMessage = mapList(msgs,
                                            msgFromValsFunc(topic));
        expectedAsMessage = modifyList(expectedAsMessage, adjustKey());
        List<String> expectedAsString = mapList(expectedAsMessage,
                                            msgToJSONStringFunc());

        completeAndValidate(rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    @Test
    public void testImplicitTopicProducer() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testImplicitTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String topic = getKafkaTopics()[0];

        ProducerConnector producer = new ProducerConnector(top, createProducerConfig());
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig());
        
        // Test producer that takes an arbitrary TStream<T> and implicit topic
        
        List<Vals> msgs = new ArrayList<>();
        msgs.add(new Vals(mgen.create(topic, "Hello"), null, null));
        msgs.add(new Vals(mgen.create(topic, "key1", "Are you there?"), "key1", null));
        
        TStream<Vals> valsToPublish = top.constants(msgs);
        
        TStream<Message> msgsToPublish = valsToPublish.transform(msgFromValsFunc(topic));
        
        producer.publish(msgsToPublish);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        List<Message> expectedAsMessage = mapList(msgs,
                                            msgFromValsFunc(topic));
        List<String> expectedAsString = mapList(expectedAsMessage,
                                            msgToJSONStringFunc());

        completeAndValidate(rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    @Test
    public void testMsgImplProducer() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testMsgImplProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String topic = getKafkaTopics()[0];

        ProducerConnector producer = new ProducerConnector(top, createProducerConfig());
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig());
        
        // Test producer that takes TStream<SimpleMessage> and an explicit topic.
        
        List<Message> msgs = new ArrayList<>();
        msgs.add(new SimpleMessage(mgen.create(topic, "Hello")));
        msgs.add(new SimpleMessage(mgen.create(topic, "key1", "Are you there?"), "key1"));
        
        TStream<Message> msgsToPublish = top.constants(msgs);
        
        producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        msgs = modifyList(msgs, setTopic(topic));
        List<String> expectedAsString = mapList(msgs,
                                            msgToJSONStringFunc());

        completeAndValidate(rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    @Test
    public void testSubtypeExplicitTopicProducer() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testSubtypeExplicitTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String topic = getKafkaTopics()[0];

        ProducerConnector producer = new ProducerConnector(top, createProducerConfig());
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig());
       
        // Test producer that takes a TStream<MyMsgSubtype>
        
        List<MyMsgSubtype> msgs = new ArrayList<>();
        msgs.add(new MyMsgSubtype(mgen.create(topic, "Hello")));
        msgs.add(new MyMsgSubtype(mgen.create(topic, "key1", "Are you there?"), "key1"));
        
        TStream<MyMsgSubtype> msgsToPublish = top.constants(msgs);
        
        producer.publish(msgsToPublish, topic);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        List<String> expectedAsString = mapList(msgs,
                                            subtypeMsgToJSONStringFunc(topic));

        completeAndValidate(rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    @Test
    public void testSubtypeImplicitTopicProducer() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testSubtypeImplicitTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String topic = getKafkaTopics()[0];

        ProducerConnector producer = new ProducerConnector(top, createProducerConfig());
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig());
        
        // Test producer that takes a TStream<MyMsgSubtype> implicit topic
        
        List<MyMsgSubtype> msgs = new ArrayList<>();
        msgs.add(new MyMsgSubtype(mgen.create(topic, "Hello"), null, topic));
        msgs.add(new MyMsgSubtype(mgen.create(topic, "key1", "Are you there?"), "key1", topic));
        
        TStream<MyMsgSubtype> msgsToPublish = top.constants(msgs);
        
        producer.publish(msgsToPublish);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        List<String> expectedAsString = mapList(msgs,
                                            subtypeMsgToJSONStringFunc(null));

        completeAndValidate(rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    @Test
    public void testMultiTopicProducer() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testMultiTopicProducer");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String[] topics = getKafkaTopics();
        String topic1 = topics[0];
        String topic2 = topics[1];

        ProducerConnector producer = new ProducerConnector(top, createProducerConfig());
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig());
        
        // Test producer that publishes to multiple topics (implies implicit topic)
        
        List<Message> topic1Msgs = new ArrayList<>();
        topic1Msgs.add(new SimpleMessage(mgen.create(topic1, "Hello"), null, topic1));
        topic1Msgs.add(new SimpleMessage(mgen.create(topic1, "Are you there?"), null, topic1));
        
        List<Message> topic2Msgs = new ArrayList<>();
        topic2Msgs.add(new SimpleMessage(mgen.create(topic2, "Hello"), null, topic2));
        topic2Msgs.add(new SimpleMessage(mgen.create(topic2, "Are you there?"), null, topic2));
        
        List<Message> msgs = new ArrayList<>(topic1Msgs);
        msgs.addAll(topic2Msgs);
        
        TStream<Message> msgsToPublish = top.constants(msgs);
        
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
                                            
        completeAndValidateUnordered(top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    @Test
    public void testMultiTopicConsumer() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testMultiTopicConsumer");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String[] topics = getKafkaTopics();
        String topic1 = topics[0];
        String topic2 = topics[1];

        ProducerConnector producer = new ProducerConnector(top, createProducerConfig());
        ConsumerConnector consumer = new ConsumerConnector(top, createConsumerConfig());
      
        // Test consumer that receives from multiple topics
        
        List<Message> topic1Msgs = new ArrayList<>();
        topic1Msgs.add(new SimpleMessage(mgen.create(topic1, "Hello"), "mykey1", topic1));
        topic1Msgs.add(new SimpleMessage(mgen.create(topic1, "Are you there?"), "mykey2", topic1));
        
        List<Message> topic2Msgs = new ArrayList<>();
        topic2Msgs.add(new SimpleMessage(mgen.create(topic2, "Hello"), "mykey1", topic2));
        topic2Msgs.add(new SimpleMessage(mgen.create(topic2, "Are you there?"), "mykey2", topic2));
        
        List<Message> msgs = new ArrayList<>(topic1Msgs);
        msgs.addAll(topic2Msgs);
        
        TStream<Message> topic1MsgsToPublish = top.constants(topic1Msgs);
        producer.publish(topic1MsgsToPublish);
        
        TStream<Message> topic2MsgsToPublish = top.constants(topic2Msgs);
        producer.publish(topic2MsgsToPublish);
        
        TStream<Message> rcvdMsgs = consumer.subscribe(topics);
        
        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(msgToJSONStringFunc());
        List<String> expectedAsString = mapList(msgs,
                                            msgToJSONStringFunc());

        completeAndValidateUnordered(top, rcvdAsString, SEC_TIMEOUT, expectedAsString.toArray(new String[0]));
    }
    
    // would be nice if Tester provided this too
    private void completeAndValidateUnordered(Topology topology,
            TStream<String> stream, int secTimeout, String... expected)
            throws Exception {
        
        Tester tester = topology.getTester();

        Condition<Long> sCount = tester.tupleCount(stream, expected.length);
        Condition<List<String>> sContents = tester.stringContentsUnordered(stream, expected);

        complete(tester, sCount, secTimeout, TimeUnit.SECONDS);

        assertTrue("contents:" + sContents, sContents.valid());
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
