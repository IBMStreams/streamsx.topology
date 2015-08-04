/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.distributed;

import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streams.operator.types.XML;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.spl.SPLSchemas;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;

/**
 * Test publish/subscribe. These tests just use publish/subscribe
 * within a single job, but the expected use case is across jobs.
 *
 */
public class PublishSubscribeTest extends TestTopology {

    @Before
    public void checkIsDistributed() {
        assumeTrue(getTesterType() == Type.DISTRIBUTED_TESTER);
    }

    @Test
    public void testPublishString() throws Exception {
        final Topology t = new Topology();
        TStream<String> source = t.strings("325", "457", "9325");
        
        source = source.modify(new Delay<String>());
        
        source.publish("/testPublishString");
        
        TStream<String> subscribe = t.subscribe("/testPublishString", String.class);

        completeAndValidate(subscribe, 20, "325", "457", "9325");
    }
    
    @Test
    public void testPublishStringMultipleTopics() throws Exception {
        final Topology t = new Topology();
        TStream<String> source = t.strings("325", "457", "9325");       
        source = source.modify(new Delay<String>());       
        source.publish("/testPublishString");
        
        // A stream that should not be subscribed to!
        TStream<String> source2 = t.strings("999", "777", "8888");       
        source2 = source2.modify(new Delay<String>());       
        source2.publish("/testPublishString2");
 
        
        TStream<String> subscribe = t.subscribe("/testPublishString", String.class);

        completeAndValidate(subscribe, 20, "325", "457", "9325");
    }
    
    // Test a exported String Java stream can be subscribed to by a SPL stream.
    @Test
    public void testPublishStringToSPL() throws Exception {
        final Topology t = new Topology();
        TStream<String> source = t.strings("hello", "SPL!");
        
        source = source.modify(new Delay<String>());
        
        source.publish("/testPublishStringSPL");
        
        SPLStream subscribe = SPLStreams.subscribe(t, "/testPublishStringSPL", SPLSchemas.STRING);        

        completeAndValidate(subscribe.toStringStream(), 20, "hello", "SPL!");
    }
    
    
    @Test
    public void testPublishBlob() throws Exception {
        TStream<String> strings = publishBlobTopology();
        
        completeAndValidate(strings, 20, "93245", "hello", "was a blob!");
    }
    
    @SuppressWarnings("serial")
    public static TStream<String> publishBlobTopology() throws Exception {
    
        final Topology t = new Topology();
        TStream<String> source = t.strings("93245", "hello", "was a blob!");       
        source = source.modify(new Delay<String>());
        
        TStream<Blob> blobs = source.transform(new Function<String,Blob>() {

            @Override
            public Blob apply(String v) {
                return ValueFactory.newBlob(v.getBytes(StandardCharsets.UTF_8));
            }});
        
        blobs.publish("/testPublishBlob");
        
        TStream<Blob> subscribe = t.subscribe("/testPublishBlob", Blob.class);
        
        TStream<String> strings = subscribe.transform(new Function<Blob,String>() {

            @Override
            public String apply(Blob v) {
                return new String(v.getData(), StandardCharsets.UTF_8);
            }});      

        return strings;
    }
    
    @Test
    public void testPublishXML() throws Exception {
        TStream<String> strings = publishXMLTopology();
        
        completeAndValidate(strings, 20, "<book>Catch 22</book>", "<bus>V</bus>");
    }
    
    @SuppressWarnings("serial")
    public static TStream<String> publishXMLTopology() throws Exception {
    
        final Topology t = new Topology();
        TStream<String> source = t.strings("<book>Catch 22</book>", "<bus>V</bus>");       
        source = source.modify(new Delay<String>());
        
        TStream<XML> xml = source.transform(new Function<String,XML>() {

            @Override
            public XML apply(String v) {
                try {
                    return ValueFactory.newXML(new ByteArrayInputStream(v.getBytes(StandardCharsets.UTF_8)));
                } catch (IOException e) {
                    return null;
                }
            }});
        
        xml.publish("/testPublishXML");
        
        TStream<XML> subscribe = t.subscribe("/testPublishXML", XML.class);
        
        TStream<String> strings = subscribe.transform(new Function<XML,String>() {

            @Override
            public String apply(XML v) {
                byte[] data = new byte[100];
                InputStream in = v.getInputStream();
                int read;
                try {
                    read = in.read(data);
                } catch (IOException e) {
                    return null;
                }
                return new String(data, 0, read, StandardCharsets.UTF_8);
            }});      

        return strings;
    }
    
    @Test
    public void testPublishJavaObject() throws Exception {
        TStream<String> strings = publishJavaObjectTopology();
        
        completeAndValidate(strings, 20, "publishing", "a", "java object");
    }
    
    @SuppressWarnings("serial")
    public static TStream<String> publishJavaObjectTopology() throws Exception {
    
        final Topology t = new Topology();
        TStream<String> source = t.strings("publishing", "a", "java object");       
        source = source.modify(new Delay<String>());
        
        TStream<SimpleString> objects = source.transform(new Function<String,SimpleString>() {

            @Override
            public SimpleString apply(String v) {
                return new SimpleString(v);
            }});
        
        objects.publish("/testPublishJavaObject");
        
        TStream<SimpleString> subscribe = t.subscribe("/testPublishJavaObject", SimpleString.class);
        
        TStream<String> strings = StringStreams.toString(subscribe);  

        return strings;
    }
    
    /**
     * Publish two Java object streams to the same topic
     * but ensure that the subscriber selects the correct one
     * based upon type.
     */
    @SuppressWarnings("serial")
    public static TStream<String> publishJavaObjectMultiple() throws Exception {
    
        final Topology t = new Topology();
        List<SimpleInt> ints = new ArrayList<>();
        ints.add(new SimpleInt(0));
        ints.add(new SimpleInt(1));
        ints.add(new SimpleInt(2));
        ints.add(new SimpleInt(3));
        TStream<SimpleInt> sints = t.constants(ints).asType(SimpleInt.class);
        sints = sints.modify(new Delay<SimpleInt>());
        sints.publish("/testPublishJavaObjects");
        
        TStream<String> source = t.strings("publishing", "a", "java object");       
        source = source.modify(new Delay<String>());
        
        TStream<SimpleString> objects = source.transform(new Function<String,SimpleString>() {

            @Override
            public SimpleString apply(String v) {
                return new SimpleString(v);
            }}).asType(SimpleString.class);
        
        objects.publish("/testPublishJavaObjects");
                
        TStream<SimpleString> subscribe = t.subscribe("/testPublishJavaObjects", SimpleString.class);
        
        TStream<String> strings = StringStreams.toString(subscribe);  

        return strings;
    }
    
    /**
     * Delay to ensure that tuples are not dropped while dynamic
     * connections are being made.
     */
    @SuppressWarnings("serial")
    public static class Delay<T> implements UnaryOperator<T> {
        
        private boolean first = true;

        @Override
        public T apply(T v)  {
            if (first) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    return null;
                }
                first = false;
            }
            
            return v;
        }
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
}
