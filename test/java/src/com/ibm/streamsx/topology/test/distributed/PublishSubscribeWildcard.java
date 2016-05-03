/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.distributed;

import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.test.TestTopology;

/**
 * Test publish/subscribe. These tests just use publish/subscribe
 * within a single job, but the expected use case is across jobs.
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
        wildcardSingle("a", "+");
    }
    
    @Test
    public void testPlusFilter2() throws Exception {
        wildcardSingle("a/b", "+/b");
    }
    @Test
    public void testPlusFilter3() throws Exception {
        wildcardSingle("a/b", "a/+");
    }
    @Test
    public void testPlusFilter4() throws Exception {
        wildcardSingle("/b", "+/b");
    }
    @Test
    public void testPlusFilter5() throws Exception {
        wildcardSingle("a/b/c", "a/+/c");
    }
    @Test
    public void testPlusFilter6() throws Exception {
        wildcardSingle("a/b/c", "a/b/+");
    }
    @Test
    public void testPlusFilter7() throws Exception {
        wildcardSingle("d/e/f", "+/e/+");
    }
    @Test
    public void testPlusFilter8() throws Exception {
        wildcardSingle("d/e/f", "+/+/+");
    }
    @Test
    public void testPlusFilter9() throws Exception {
        wildcardSingle("d//f", "d/+/f");
    }
    @Test
    public void testPlusFilter10() throws Exception {
        wildcardSingle("/c/", "+/c/");
    }
    
    
    /**
     * Tests subscribing to a single stream with a wildcard filter.
     * @param topicName
     * @param topicFilter
     * @throws Exception
     */
    private void wildcardSingle(String topicName, String topicFilter) throws Exception {
        
        Topology t = new Topology();
        
        List<String> data = publish(t, topicName);
        
        TStream<String> subscribe = t.subscribe(topicFilter, String.class);
        
        completeAndValidate(subscribe, 30, data.toArray(new String[0]));
    }
    
    
    private List<String> publish(Topology t, String topic) {
        int base = sbase;
        sbase +=200;
        List<String> data = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            data.add("P" + base + rand.nextInt(100));
        }
        
        t.constants(data).asType(String.class).modify(new PublishSubscribeTest.Delay<>()).publish(topic);
        return data;
    }
}
