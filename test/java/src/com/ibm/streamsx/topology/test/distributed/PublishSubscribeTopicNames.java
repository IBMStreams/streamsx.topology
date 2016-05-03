/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.distributed;

import org.junit.Test;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.test.TestTopology;

/**
 * Test publish/subscribe. These tests just use publish/subscribe
 * within a single job, but the expected use case is across jobs.
 *
 */
public class PublishSubscribeTopicNames extends TestTopology {

  
    @Test(expected=NullPointerException.class)
    public void testNullTopicName() throws Exception {
        final Topology t = new Topology();
       
        t.strings().publish(null, true);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testEmptyTopicName() throws Exception {
        final Topology t = new Topology();
       
        t.strings().publish("", true);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testNulCharTopicName() throws Exception {
        final Topology t = new Topology();
       
        t.strings().publish("a\u0000b", true);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testWildcardPlus1TopicName() throws Exception {
        final Topology t = new Topology();
       
        t.strings().publish("+", true);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testWildcardPlus2TopicName() throws Exception {
        final Topology t = new Topology();
       
        t.strings().publish("engine/+", true);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testWildcardHash1TopicName() throws Exception {
        final Topology t = new Topology();
       
        t.strings().publish("engine/#", true);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testWildcardHash2TopicName() throws Exception {
        final Topology t = new Topology();
       
        t.strings().publish("#", true);
    }
    
}
