/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.kafka;

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
import com.ibm.streamsx.topology.kafka.KafkaMessage;
import com.ibm.streamsx.topology.kafka.KafkaStreams;
import com.ibm.streamsx.topology.kafka.KafkaMessageImpl;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;


/**
 * N.B. Optional configuration properties:
 * <ul>
 * <li>{@code com.ibm.streamsx.topology.test.kafka.csvTopics} the
 *     pre-existing kafka topic(s) to use. Defaults to "testTopic1,testTopic2".</li>
 * <li>{@code com.ibm.streamsx.topology.test.kafka.zookeeper.connect} the
 *      Kafka cluster's zookeeper connection string. Defaults to "localhost:2181".</li>
 * <li>{@code com.ibm.streamsx.topology.test.kafka.metadata.broker.list} the
 *      Kafka cluster's brokers addresses. Defaults to "localhost:9092".</li>
 * </ul>
 */
public class KafkaStreamsTest extends TestTopology {
    
    private static List<Vals> testMsgVals = new ArrayList<>();
    static {
        testMsgVals.add(new Vals("myMsg", "myKey", "myTopic"));
        testMsgVals.add(new Vals("myMsg", null, "myTopic"));
        testMsgVals.add(new Vals("myMsg", "myKey", null));
        testMsgVals.add(new Vals("myMsg", null, null));
    }
    
    /**
     * Class with no type relationship to KafkaMessage
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
     * Class that implements KafkaMessage
     */
    private static class MyMsgSubtype implements KafkaMessage {
        private static final long serialVersionUID = 1L;
        private String more = "I am a KafkaTupleSubtype";
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
        public String getKafkaMessage() { return msg; }
        @Override
        public String getKafkaKey() { return key; }
        @Override
        public String getKafkaTopic() { return topic; }
        @Override
        public String toString() { return "["+more+"] "+super.toString(); }
    }
    
    private String[] getKafkaTopics() {
        String csvTopics = System.getProperty("com.ibm.streamsx.topology.test.kafka.csvTopics", "testTopic1,testTopic2");
        String[] topics = csvTopics.split(",");
        return topics;
    }
    
    private String getKafkaZookeeperConnect() {
        return System.getProperty("com.ibm.streamsx.topology.test.kafka.zookeeper.connect", "localhost:2181");
    }
    
    private String getKafkaMetadataBrokerList() {
        return System.getProperty("com.ibm.streamsx.topology.test.kafka.metedata.broker.list", "localhost:9092");
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
    public void testKafkaMessageImpl() throws Exception {
        // test ctors
        for (Vals vals : testMsgVals) {
            KafkaMessageImpl t = !vals.hasTopic() ? new KafkaMessageImpl(vals.msg, vals.key)
                            : new KafkaMessageImpl(vals.msg, vals.key, vals.topic);
            assertEquals("ctor "+vals.toString(), vals.key, t.getKafkaKey());
            assertEquals("ctor "+vals.toString(), vals.msg, t.getKafkaMessage());
            assertEquals("ctor "+vals.toString(), vals.topic, t.getKafkaTopic());
            if (!vals.hasKey() && !vals.hasTopic()) {
                t = new KafkaMessageImpl(vals.msg);
                assertEquals("ctor "+vals.toString(), vals.key, t.getKafkaKey());
                assertEquals("ctor "+vals.toString(), vals.msg, t.getKafkaMessage());
                assertEquals("ctor "+vals.toString(), vals.topic, t.getKafkaTopic());
            }
        }
        // test equals()
        for (Vals vals : testMsgVals) {
            KafkaMessage t = !vals.hasTopic() ? new KafkaMessageImpl(vals.msg, vals.key)
                            : new KafkaMessageImpl(vals.msg, vals.key, vals.topic);
            KafkaMessage t2 = !vals.hasTopic() ? new KafkaMessageImpl(vals.msg, vals.key)
                            : new KafkaMessageImpl(vals.msg, vals.key, vals.topic);
            assertTrue("same equals() - "+t.toString(), t.equals(t));
            assertTrue("equals() - "+t.toString(), t.equals(t2));
        }
        // test equals() negative
        for (Vals vals : testMsgVals) {
            KafkaMessage t = !vals.hasTopic() ? new KafkaMessageImpl(vals.msg, vals.key)
                             : new KafkaMessageImpl(vals.msg, vals.key, vals.topic);
            KafkaMessage t2 = !vals.hasTopic() ? new KafkaMessageImpl("someOtherValue", "someOtherKey")
                            : new KafkaMessageImpl("someOtherValue", "someOtherKey", "someOtherTopic");
            assertFalse("neg equals() - "+t.toString(), t.equals(t2));

            t2 = new KafkaMessageImpl(vals.msg, vals.key, "someOtherTopic");
            assertFalse("neg equals() - "+t.toString(), t.equals(t2));
            
            t2 = new KafkaMessageImpl("someOtherValue", vals.key);
            assertFalse("neg equals() - "+t.toString(), t.equals(t2));
            
            t2 = new KafkaMessageImpl(vals.msg, "someOtherKey");
            assertFalse("neg equals() - "+t.toString(), t.equals(t2));
            
            assertFalse("neg equals() - "+t.toString(), t.equals(null));
            assertFalse("neg equals() - "+t.toString(), t.equals("notAKafkaTupleObject"));
        }
    }
    
    private void checkAssumes() {
        // Embedded and KafkaStreams implementation leveraging 
        // SPL kafkaProducer,Consumer ops don't mix.
        assumeTrue(!isEmbedded());
        
        // Standalone doesn't work at least with the current test harness.
        // submit()/sc fails due to incompatible VmArgs in a single PE
        assumeTrue(getTesterType() != Type.STANDALONE_TESTER);
        
        assumeTrue(SC_OK);
    }
    
    @Test
    public void testGenericExplicitTopicSink() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testGenericExplicitTopicSink");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String topic = getKafkaTopics()[0];
        
        // Test sink that takes an arbitrary TStream<T> and explicit topic
        
        List<Vals> msgs = new ArrayList<>();
        msgs.add(new Vals(mgen.create(topic, "Hello"), null, null));
        msgs.add(new Vals(mgen.create(topic, "key1", "Are you there?"), "key1", null));
        
        TStream<Vals> msgsToPublish = top.constants(msgs, Vals.class);
        
        KafkaStreams.sink(top, createProducerConfig(),
                                topic, msgsToPublish,
                                msgFromValsFunc(null));
        
        TStream<KafkaMessage> rcvdMsgs = KafkaStreams.source(top,
                                createConsumerConfig(), topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(
                                            msgToJSONStringFunc(), String.class);
        List<KafkaMessage> expectedAsKafka = mapList(msgs,
                                            msgFromValsFunc(null));
        List<String> expectedAsStringList = mapList(expectedAsKafka,
                                            msgToJSONStringFunc());

        completeAndValidate(rcvdAsString, 10, expectedAsStringList.toArray(new String[0]));
    }
    
    @Test
    public void testGenericImplicitTopicSink() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testGenericImplicitTopicSink");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String topic = getKafkaTopics()[0];
        
        // Test sink that takes an arbitrary TStream<T> and implicit topic
        
        List<Vals> msgs = new ArrayList<>();
        msgs.add(new Vals(mgen.create(topic, "Hello"), null, null));
        msgs.add(new Vals(mgen.create(topic, "key1", "Are you there?"), "key1", null));
        
        TStream<Vals> msgsToPublish = top.constants(msgs, Vals.class);
        
        KafkaStreams.sink(top, createProducerConfig(),
                                null/*topic*/, msgsToPublish,
                                msgFromValsFunc(topic));
        
        TStream<KafkaMessage> rcvdMsgs = KafkaStreams.source(top,
                                createConsumerConfig(), topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(
                                            msgToJSONStringFunc(), String.class);
        List<KafkaMessage> expectedAsKafka = mapList(msgs,
                                            msgFromValsFunc(null));
        List<String> expectedAsStringList = mapList(expectedAsKafka,
                                            msgToJSONStringFunc());

        completeAndValidate(rcvdAsString, 10, expectedAsStringList.toArray(new String[0]));
    }
    
    @Test
    public void testMsgImplSink() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testMsgImplSink");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String topic = getKafkaTopics()[0];
        
        // Test sink that takes TStream<KafkaMessageImpl> and an explicit topic.
        
        List<KafkaMessage> msgs = new ArrayList<>();
        msgs.add(new KafkaMessageImpl(mgen.create(topic, "Hello")));
        msgs.add(new KafkaMessageImpl(mgen.create(topic, "key1", "Are you there?"), "key1"));
        
        TStream<KafkaMessage> msgsToPublish = top.constants(msgs, KafkaMessage.class);
        
        KafkaStreams.sink(top, createProducerConfig(),
                                topic, msgsToPublish);
        
        TStream<KafkaMessage> rcvdMsgs = KafkaStreams.source(top,
                                createConsumerConfig(), topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(
                                            msgToJSONStringFunc(), String.class);
        List<String> expectedAsStringList = mapList(msgs,
                                            msgToJSONStringFunc());

        completeAndValidate(rcvdAsString, 10, expectedAsStringList.toArray(new String[0]));
    }
    
    @Test
    public void testSubtypeExplicitTopicSink() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testSubtypeExplicitTopicSink");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String topic = getKafkaTopics()[0];
        
        // Test sink that takes a TStream<MyMsgSubtype>
        
        List<MyMsgSubtype> msgs = new ArrayList<>();
        msgs.add(new MyMsgSubtype(mgen.create(topic, "Hello")));
        msgs.add(new MyMsgSubtype(mgen.create(topic, "key1", "Are you there?"), "key1"));
        
        TStream<MyMsgSubtype> msgsToPublish = top.constants(msgs, MyMsgSubtype.class);
        
        KafkaStreams.sink(top, createProducerConfig(),
                                topic, msgsToPublish);
        
        TStream<KafkaMessage> rcvdMsgs = KafkaStreams.source(top,
                                createConsumerConfig(), topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(
                                            msgToJSONStringFunc(), String.class);
        List<String> expectedAsStringList = mapList(msgs,
                                            subtypeMsgToJSONStringFunc());

        completeAndValidate(rcvdAsString, 10, expectedAsStringList.toArray(new String[0]));
    }
    
    @Test
    public void testSubtypeImplicitTopicSink() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testSubtypeImplicitTopicSink");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String topic = getKafkaTopics()[0];
        
        // Test sink that takes a TStream<MyMsgSubtype> implicit topic
        
        List<MyMsgSubtype> msgs = new ArrayList<>();
        msgs.add(new MyMsgSubtype(mgen.create(topic, "Hello"), null, topic));
        msgs.add(new MyMsgSubtype(mgen.create(topic, "key1", "Are you there?"), "key1", topic));
        
        TStream<MyMsgSubtype> msgsToPublish = top.constants(msgs, MyMsgSubtype.class);
        
        KafkaStreams.sink(top, createProducerConfig(),
                                null/*topic*/, msgsToPublish);
        
        TStream<KafkaMessage> rcvdMsgs = KafkaStreams.source(top,
                                createConsumerConfig(), topic);

        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        rcvdMsgs = addTopic(rcvdMsgs, topic);
        TStream<String> rcvdAsString = rcvdMsgs.transform(
                                            msgToJSONStringFunc(), String.class);
        List<String> expectedAsStringList = mapList(msgs,
                                            subtypeMsgToJSONStringFunc());

        completeAndValidate(rcvdAsString, 10, expectedAsStringList.toArray(new String[0]));
    }
    
    @Test
    public void testMultiTopicSink() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testMultiTopicSink");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String[] topics = getKafkaTopics();
        String topic1 = topics[0];
        String topic2 = topics[1];
        
        // Test sink that publishes to multiple topics (implies implicit topic)
        
        List<KafkaMessage> topic1Msgs = new ArrayList<>();
        topic1Msgs.add(new KafkaMessageImpl(mgen.create(topic1, "Hello"), null, topic1));
        topic1Msgs.add(new KafkaMessageImpl(mgen.create(topic1, "Are you there?"), null, topic1));
        
        List<KafkaMessage> topic2Msgs = new ArrayList<>();
        topic2Msgs.add(new KafkaMessageImpl(mgen.create(topic2, "Hello"), null, topic2));
        topic2Msgs.add(new KafkaMessageImpl(mgen.create(topic2, "Are you there?"), null, topic2));
        
        List<KafkaMessage> msgs = new ArrayList<>(topic1Msgs);
        msgs.addAll(topic2Msgs);
        
        TStream<KafkaMessage> msgsToPublish = top.constants(msgs, KafkaMessage.class);
        
        KafkaStreams.sink(top, createProducerConfig(),
                                null/*topic*/, msgsToPublish);
        
        TStream<KafkaMessage> rcvdTopic1Msgs = KafkaStreams.source(top,
                                createConsumerConfig(), topic1);
        
        TStream<KafkaMessage> rcvdTopic2Msgs = KafkaStreams.source(top,
                                createConsumerConfig(), topic2);
        
        // for validation...
        
        rcvdTopic1Msgs = addTopic(rcvdTopic1Msgs, topic1);
        rcvdTopic2Msgs = addTopic(rcvdTopic2Msgs, topic2);
        TStream<KafkaMessage> rcvdMsgs = rcvdTopic1Msgs.union(rcvdTopic2Msgs);
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(
                                            msgToJSONStringFunc(), String.class);
        List<String> expectedAsStringList = mapList(msgs,
                                            msgToJSONStringFunc());
                                            
        completeAndValidateUnordered(top, rcvdAsString, 10, expectedAsStringList.toArray(new String[0]));
    }
    
    @Test
    public void testMultiTopicSource() throws Exception {
        
        checkAssumes();
        
        Topology top = new Topology("testMultiTopicSource");
        MsgGenerator mgen = new MsgGenerator(top.getName());

        String[] topics = getKafkaTopics();
        String topic1 = topics[0];
        String topic2 = topics[1];
        
        // Test source that receives from multiple topics
        
        List<KafkaMessage> topic1Msgs = new ArrayList<>();
        topic1Msgs.add(new KafkaMessageImpl(mgen.create(topic1, "Hello")));
        topic1Msgs.add(new KafkaMessageImpl(mgen.create(topic1, "Are you there?")));
        
        List<KafkaMessage> topic2Msgs = new ArrayList<>();
        topic2Msgs.add(new KafkaMessageImpl(mgen.create(topic2, "Hello")));
        topic2Msgs.add(new KafkaMessageImpl(mgen.create(topic2, "Are you there?")));
        
        List<KafkaMessage> msgs = new ArrayList<>(topic1Msgs);
        msgs.addAll(topic2Msgs);
        
        TStream<KafkaMessage> topic1MsgsToPublish = top.constants(topic1Msgs, KafkaMessage.class);
        KafkaStreams.sink(top, createProducerConfig(),
                                topic1, topic1MsgsToPublish);
        
        TStream<KafkaMessage> topic2MsgsToPublish = top.constants(topic2Msgs, KafkaMessage.class);
        KafkaStreams.sink(top, createProducerConfig(),
                                topic2, topic2MsgsToPublish);
        
        TStream<KafkaMessage> rcvdMsgs = KafkaStreams.source(top,
                                createConsumerConfig(), topics);
        
        // for validation...
        rcvdMsgs.print();
        rcvdMsgs = selectMsgs(rcvdMsgs, mgen.pattern()); // just our msgs
        TStream<String> rcvdAsString = rcvdMsgs.transform(
                                            msgToJSONStringFunc(), String.class);
        List<String> expectedAsStringList = mapList(msgs,
                                            msgToJSONStringFunc());

        completeAndValidateUnordered(top, rcvdAsString, 10, expectedAsStringList.toArray(new String[0]));
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

    private static TStream<KafkaMessage> addTopic(TStream<KafkaMessage> stream,
                                            final String topic) { 
        return stream.transform(
            new Function<KafkaMessage,KafkaMessage>() {
                private static final long serialVersionUID = 1L;
    
                @Override
                public KafkaMessage apply(KafkaMessage v) {
                    return new KafkaMessageImpl(v.getKafkaMessage(), v.getKafkaKey(), topic);
                }
            },
            KafkaMessage.class);
    }

    private static TStream<KafkaMessage> selectMsgs(TStream<KafkaMessage> stream,
                                            final String pattern) { 
        return stream.filter(
            new Predicate<KafkaMessage>() {
                private static final long serialVersionUID = 1L;
    
                @Override
                public boolean test(KafkaMessage tuple) {
                    return tuple.getKafkaMessage().matches(pattern);
                }
            });
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
    
    /**
     * Function for Vals => KafkaMessage
     * @param topic if null, KafkaMessage gets {@code Vals.topic),
     *              otherwise {@code topic}
     * @return Function<Vals,KafkaMessage>
     */
    private static Function<Vals,KafkaMessage> msgFromValsFunc(final String topic) {
        return new Function<Vals,KafkaMessage>() {
            private static final long serialVersionUID = 1L;

            @Override
            public KafkaMessage apply(Vals v) {
                if (topic!=null)
                    return new KafkaMessageImpl(v.msg, v.key, topic);
                else if (v.topic!=null)
                    return new KafkaMessageImpl(v.msg, v.key, v.topic);
                else
                    return new KafkaMessageImpl(v.msg, v.key);
            }
        };
    }

    /**
     * Function for KafkaMessage => toJSON().toString()
     */
    private static Function<KafkaMessage,String> msgToJSONStringFunc() {
        return new Function<KafkaMessage,String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String apply(KafkaMessage v) {
                return toJson(v);
            }
        };
    }

    /**
     * Function for KafkaMessageSubtype => toJSON().toString()
     */
    private static Function<MyMsgSubtype,String> subtypeMsgToJSONStringFunc() {
        return new Function<MyMsgSubtype,String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String apply(MyMsgSubtype v) {
                return toJson(v);
            }
        };
    }
    
    private static String toJson(KafkaMessage m) {
        JSONObject jo = new JSONObject();
        jo.put("key", m.getKafkaKey());
        jo.put("message", m.getKafkaMessage());
        jo.put("topic", m.getKafkaTopic());
        return jo.toString();

    }
}
