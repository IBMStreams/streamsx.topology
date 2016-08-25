/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.test.distributed;

import org.junit.Test;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.test.TestTopology;

/**
 * Test publish/subscribe invalid topic names
 * and filters.
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
    
    @Test(expected=NullPointerException.class)
    public void testNullTopicFilter() throws Exception {
        new Topology().subscribe(null, String.class);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testNulTopicFilter() throws Exception {
        new Topology().subscribe("a\u0000c", String.class);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testWildcardPlus1TopicFilter() throws Exception {
        new Topology().subscribe("a+", String.class);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testWildcardPlus2TopicFilter() throws Exception {
        new Topology().subscribe("a/+/+b", String.class);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testWildcardHash1TopicFilter() throws Exception {
        new Topology().subscribe("a#", String.class);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testWildcardHash2TopicFilter() throws Exception {
        new Topology().subscribe("a/b#", String.class);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testWildcardHash3TopicFilter() throws Exception {
        new Topology().subscribe("a/#/b", String.class);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testWildcardHash4TopicFilter() throws Exception {
        new Topology().subscribe("a/#b/c", String.class);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testWildcardHash5TopicFilter() throws Exception {
        new Topology().subscribe("a/#/c/#", String.class);
    }
    
    
}
