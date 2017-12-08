/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streams.operator.types.XML;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.jobconfig.JobConfig;
import com.ibm.streamsx.topology.spl.SPLSchemas;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * Test publish/subscribe. These tests just use publish/subscribe
 * within a single job, but the expected use case is across jobs.
 *
 */
public class PublishSubscribeTest extends TestTopology {

    @Before
    public void checkIsDistributed() {
        assumeTrue(isDistributedOrService());
    }

    @Test
    public void testPublishString() throws Exception {
        
        TStream<String> source = source();
       
        source.publish("testPublishString");
        
        TStream<String> subscribe = source.topology().subscribe("testPublishString", String.class);
        
        checkSubscribedAsStrings(subscribe);
    }
    
    @Test
    public void testPublishStringParams() throws Exception {
        
        TStream<String> source = source();
        
        String topic = "testPublishStringParams/" + System.currentTimeMillis();
       
        Supplier<String> pubParam = source.topology().createSubmissionParameter("PP", String.class);
        source.publish(pubParam);
        
        Supplier<String> subParam = source.topology().createSubmissionParameter("SP", String.class);
        TStream<String> subscribe = source.topology().subscribe(subParam, String.class);
        
        JobConfig jco = new JobConfig();
        jco.addSubmissionParameter("SP", topic);
        jco.addSubmissionParameter("PP", topic);    
        jco.addToConfig(getConfig());
        
        checkSubscribedAsStrings(subscribe);
    }
    
    private TStream<String> source() {
        return source("S");
    }
    
    private TStream<String> source(String prefix) {
        final Topology t = new Topology();
        int[] sv = new int[1];
        TStream<String> source = t.periodicSource(() -> prefix + sv[0]++,
                50, TimeUnit.MILLISECONDS);
 
        return source.asType(String.class);
    }
    
    @Test
    public void testPublishStringMultipleTopics() throws Exception {

        TStream<String> source = source();       
        source.publish("testPublishString");
        
        // A stream that should not be subscribed to!
        TStream<String> source2 = source("X");         
        source2.publish("testPublishString2");
       
        TStream<String> subscribe = source.topology().subscribe("testPublishString", String.class);

        checkSubscribedAsStrings(subscribe);
    }
    
    // Test a exported String Java stream can be subscribed to by a SPL stream.
    @Test
    public void testPublishStringToSPL() throws Exception {
        TStream<String> source = source();
        
        // Check autonomous works in that it produces working SPL code.
        source = source.autonomous();
        assertEquals(String.class, source.getTupleClass());
        assertEquals(String.class, source.getTupleType());
        
        source.publish("testPublishStringSPL");
        
        SPLStream subscribe = SPLStreams.subscribe(source.topology(), "testPublishStringSPL", SPLSchemas.STRING);        

        checkSubscribedAsStrings(subscribe.toStringStream());
    }    
    
    @Test
    public void testPublishBlob() throws Exception {
        // Requires Blob class
        assumeTrue(hasStreamsInstall());
           
        TStream<Blob> blobs = source().transform(
                v -> ValueFactory.newBlob(v.getBytes(StandardCharsets.UTF_8))).asType(Blob.class);
        
        blobs.publish("testPublishBlob");
        
        TStream<Blob> subscribe = blobs.topology().subscribe("testPublishBlob", Blob.class);
        
        TStream<String> strings = subscribe.transform(v -> new String(v.getData(), StandardCharsets.UTF_8));

        checkSubscribedAsStrings(strings);
    }

    private void checkSubscribedAsStrings(TStream<String> strings) throws Exception {
        
        Topology t = strings.topology();
                
        Condition<Long> atLeast = t.getTester().atLeastTupleCount(strings, 100);
        Condition<List<String>> subTuples = t.getTester().stringContents(strings);
        
        complete(t.getTester(), atLeast, 30, TimeUnit.SECONDS);
        
        assertTrue(atLeast.valid());
        
        // Can't get results from the Condition.
        if (isStreamingAnalyticsRun())
            return;

        List<String> result = subTuples.getResult();
        assertFalse(result.isEmpty());
        int last = -1;
        for (String r : result) {
            assertEquals('S', r.charAt(0));
            int v = Integer.valueOf(r.substring(1));
            if (last != -1)
                assertEquals(last+1, v);
            last = v;
        }
    }
    
    @Test
    public void testPublishXML() throws Exception {
        // Requires XML class
        assumeTrue(hasStreamsInstall());
        
        TStream<String> source = source();
        
        source = source.transform(
                s -> "<a>" + s + "</a>");
        
        TStream<XML> xml = source.transform(
                v -> { try { return ValueFactory.newXML(new ByteArrayInputStream(v.getBytes(StandardCharsets.UTF_8)));
                } catch (IOException e) {
                    return null;
                }
            }).asType(XML.class);
        
        xml.publish("testPublishXML");
        
        TStream<XML> subscribe = source.topology().subscribe("testPublishXML", XML.class);
        
        TStream<String> strings = subscribe.transform(v-> {
                byte[] data = new byte[100];
                InputStream in = v.getInputStream();
                int read;
                try {
                    read = in.read(data);
                } catch (IOException e) {
                    return null;
                }
                return new String(data, 0, read, StandardCharsets.UTF_8);
            });
        
        strings = strings.transform(s -> s.substring(3, s.length() - 4));

        checkSubscribedAsStrings(strings);
    }
    
    @Test
    public void testPublishJavaObject() throws Exception {
        TStream<String> source = source();
        
        TStream<SimpleString> objects = source.transform(SimpleString::new).asType(SimpleString.class);
        
        objects.publish("testPublishJavaObject");
        
        TStream<SimpleString> subscribe = source.topology().subscribe("testPublishJavaObject", SimpleString.class);
        
        TStream<String> strings = StringStreams.toString(subscribe);  
        
        checkSubscribedAsStrings(strings);
    }
    
    /**
     * Publish two Java object streams to the same topic
     * but ensure that the subscriber selects the correct one
     * based upon type.
     */
    public TStream<String> publishJavaObjectMultiple() throws Exception {
    
        final Topology t = new Topology();
        List<SimpleInt> ints = new ArrayList<>();
        ints.add(new SimpleInt(0));
        ints.add(new SimpleInt(1));
        ints.add(new SimpleInt(2));
        ints.add(new SimpleInt(3));
        TStream<SimpleInt> sints = t.constants(ints).asType(SimpleInt.class);
        sints = addStartupDelay(sints);
        sints.publish("testPublishJavaObjects");
        
        TStream<String> source = t.strings("publishing", "a", "java object");       
        source = addStartupDelay(source);
        
        TStream<SimpleString> objects = source.transform(SimpleString::new).asType(SimpleString.class);
        
        objects.publish("testPublishJavaObjects");
                
        TStream<SimpleString> subscribe = t.subscribe("testPublishJavaObjects", SimpleString.class);
        
        TStream<String> strings = StringStreams.toString(subscribe);  

        return strings;
    }
    
    @SuppressWarnings("serial")
    public static class SimpleString implements Serializable {
        private final String value;
        public SimpleString(String value) {
            this.value = value;
        }
        @Override
        public String toString() {
            return value;
        }
    }
    @SuppressWarnings("serial")
    public static class SimpleInt implements Serializable {
        private final int value;
        public SimpleInt(int value) {
            this.value = value;
        }
        @Override
        public String toString() {
            return "SimpleInt:" + value;
        }
    }
  
    @Test(expected=IllegalArgumentException.class)
    public void testFilterOnJava() throws Exception {
        final Topology t = new Topology();
       
        TStream<Integer> ints = t.constants(Collections.<Integer>emptyList()).asType(Integer.class);
        ints.publish("sometopic", true);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testFilterOnXML() throws Exception {
        final Topology t = new Topology();
       
        TStream<XML> xmls = t.constants(Collections.<XML>emptyList()).asType(XML.class);;
        xmls.publish("sometopic", true);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testFilterOnBlob() throws Exception {
        final Topology t = new Topology();
       
        TStream<Blob> blobs = t.constants(Collections.<Blob>emptyList()).asType(Blob.class);
        blobs.publish("sometopic", true);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testFilterOnJson() throws Exception {
        final Topology t = new Topology();
       
        TStream<JSONObject> json = t.constants(Collections.<JSONObject>emptyList()).asType(JSONObject.class);;
        json.publish("sometopic", true);
    }
}
