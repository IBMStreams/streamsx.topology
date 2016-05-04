/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.distributed;

import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.test.TestTopology;

/**
 * Test publish/subscribe with wildcard subscriptions.
 *
 */
public class PublishSubscribeWildcard extends TestTopology {
    
    private final Random rand = new Random();
    private static int sbase;

    @Before
    public void checkIsDistributed() {
        assumeTrue(getTesterType() == Type.DISTRIBUTED_TESTER);
    }
    
    @Test
    public void testNoFilter() throws Exception {
        wildcardSingle("a/b/c", "a/b/c");
    }
    
    @Test
    public void testPlusFilter1() throws Exception {
        wildcardSingle("a", "+", "a/b");
    }
    
    @Test
    public void testPlusFilter2() throws Exception {
        wildcardSingle("a/b", "+/b", "a/c");
    }
    @Test
    public void testPlusFilter3() throws Exception {
        wildcardSingle("a/b", "a/+","b/b");
    }
    @Test
    public void testPlusFilter4() throws Exception {
        wildcardSingle("/b", "+/b", "/c");
    }
    @Test
    public void testPlusFilter5() throws Exception {
        wildcardSingle("a/b/c", "a/+/c", "d/b/c");
    }
    @Test
    public void testPlusFilter6() throws Exception {
        wildcardSingle("a/b/c", "a/b/+", "d/b/c");
    }
    @Test
    public void testPlusFilter7() throws Exception {
        wildcardSingle("d/e/f", "+/e/+", "d/a/f");
    }
    @Test
    public void testPlusFilter8() throws Exception {
        wildcardSingle("d/e/f", "+/+/+", "d/a/f/");
    }
    @Test
    public void testPlusFilter9() throws Exception {
        wildcardSingle("d//f", "d/+/f", "e//f");
    }
    @Test
    public void testPlusFilter10() throws Exception {
        wildcardSingle("/c/", "+/c/", "/c/b");
    }
    
    @Test
    public void testHashAllFilter1() throws Exception {
        wildcardSingle("a", "#");
    }
    @Test
    public void testHashAllFilter2() throws Exception {
        wildcardSingle("a/c", "#");
    }
    @Test
    public void testHashAllFilter3() throws Exception {
        wildcardSingle("/", "#");
    }
    @Test
    public void testHashFilter1() throws Exception {
        wildcardSingle("a/b/c", "a/#", "/", "d", "d/f");
    }
    @Test
    public void testHashFilter2() throws Exception {
        wildcardSingle("a/b/c", "a/b/#", "a/c/e");
    }
    @Test
    public void testHashFilter3() throws Exception {
        wildcardSingle("a/b/c", "a/b/c/#", "a/b/d");
    }
    @Test
    public void testHashFilter4() throws Exception {
        wildcardSingle("//c", "//c/#", "x//c");
    }   
    
    /**
     * Tests subscribing to a single stream with a wildcard filter.
     * @param topicName
     * @param topicFilter
     * @throws Exception
     */
    private void wildcardSingle(String topicName, String topicFilter, String ...nonMatchTopics) throws Exception {
        
        Topology t = new Topology();
        
        List<String> data = publish(t, topicName, "P");
        for (String nonMatchTopic : nonMatchTopics) {
            publishPoll(t, nonMatchTopic, "X");
        }
        
        TStream<String> subscribe = t.subscribe(topicFilter, String.class);
        
        completeAndValidate(subscribe, 30, data.toArray(new String[0]));
    }
    
    
    private List<String> publish(Topology t, String topic, String leadIn) {
        int base = sbase;
        sbase +=200;
        List<String> data = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            data.add(leadIn + base + rand.nextInt(100));
        }
        
        t.constants(data).asType(String.class).modify(new PublishSubscribeTest.Delay<>()).publish(topic);
        return data;
    }
    private void publishPoll(Topology t, String topic, String leadIn) {
        final int base = sbase;
        sbase +=200;
        final String tuple = leadIn + base + rand.nextInt(100);

        t.periodicSource(() -> tuple, 200, TimeUnit.MILLISECONDS).asType(String.class).publish(topic);

    }
}
