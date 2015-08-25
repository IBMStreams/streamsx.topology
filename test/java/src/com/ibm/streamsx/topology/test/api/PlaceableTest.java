/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;

/**
 * Tests to verify Placeable
 *
 */
public class PlaceableTest {  

    @Test
    public void testSimpleTags() {
        Topology t = new Topology();        
        TStream<String> s = t.strings("3");
        
        assertTrue(s.getResourceTags().isEmpty());
        
        s.addResourceTags();
        assertTrue(s.getResourceTags().isEmpty());
        
        s.addResourceTags("ingest");        
        assertEquals(Collections.singleton("ingest"), s.getResourceTags());
        
        s.addResourceTags();
        assertEquals(Collections.singleton("ingest"), s.getResourceTags());
        
        s.addResourceTags("ingest");
        assertEquals(Collections.singleton("ingest"), s.getResourceTags());

        Set<String> expected = new HashSet<>();
        expected.add("ingest");
        expected.add("database");
        
        s.addResourceTags("database");
        assertEquals(expected, s.getResourceTags());

        expected.add("db2");
        expected.add("sales");
        s.addResourceTags("sales", "db2");
        assertEquals(expected, s.getResourceTags());
    }
    
    @Test
    public void testTagThenFuse() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");

        assertTrue(s1.getResourceTags().isEmpty());
        assertTrue(s2.getResourceTags().isEmpty());
        
        s1.addResourceTags("ingest");
        s1.fuse(s2);
        assertEquals(Collections.singleton("ingest"), s1.getResourceTags());
        assertEquals(s1.getResourceTags(), s2.getResourceTags());  
        
        Set<String> expected = new HashSet<>();
        expected.add("ingest");
        expected.add("database");
        
        s1.addResourceTags("database");
        assertEquals(expected, s1.getResourceTags());
        assertEquals(s1.getResourceTags(), s2.getResourceTags()); 
        
        expected.add("db2");
        s2.addResourceTags("db2");
        assertEquals(expected, s1.getResourceTags());
        assertEquals(s1.getResourceTags(), s2.getResourceTags());  
    }
    
    @Test
    public void testTagBothThenFuse() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");

        assertTrue(s1.getResourceTags().isEmpty());
        assertTrue(s2.getResourceTags().isEmpty());
        
        s1.addResourceTags("ingest");
        s2.addResourceTags("database");
        s1.fuse(s2);
        
        Set<String> expected = new HashSet<>();
        expected.add("ingest");
        expected.add("database");
        
        assertEquals(expected, s1.getResourceTags());
        assertEquals(s1.getResourceTags(), s2.getResourceTags()); 
    }

    @Test
    public void testFuseThenTag() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        
        assertTrue(s1.getResourceTags().isEmpty());
        assertTrue(s2.getResourceTags().isEmpty());

        s1.fuse(s2);
        assertTrue(s1.getResourceTags().isEmpty());
        assertTrue(s2.getResourceTags().isEmpty());
        
        s1.addResourceTags("ingest");
        s2.addResourceTags("database");
     
        Set<String> expected = new HashSet<>();
        expected.add("ingest");
        expected.add("database");
        
        assertEquals(expected, s1.getResourceTags());
        assertEquals(s1.getResourceTags(), s2.getResourceTags()); 
    }
}
