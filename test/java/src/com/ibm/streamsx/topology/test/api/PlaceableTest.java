/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BOperator;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.context.Placeable;
import com.ibm.streamsx.topology.streams.StringStreams;

/**
 * Tests to verify Placeable
 *
 */
public class PlaceableTest {  

    @Test
    public void testSimpleTagsStream() {
        Topology t = new Topology();        
        TStream<String> s = t.strings("3");
        testSimpleTags(s);
    }
    
    @Test
    public void testSimpleTagsSink() {
        Topology t = new Topology();        
        TStream<String> s = t.strings("3");
        testSimpleTags(s.print());
    }
    
    private void testSimpleTags(Placeable<?> s) {
        
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
    public void testTagThenFuseStream() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testTagThenFuse(s1, s2);
    }
    
    @Test
    public void testTagThenFuseSink() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testTagThenFuse(s1.print(), s2.print());
    }
    
    @Test
    public void testTagThenFuseStreamSink() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testTagThenFuse(s1, s2.print());
    }
    
    private void testTagThenFuse(Placeable<?> s1, Placeable<?> s2) {

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
    public void testTagBothThenFuseStream() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testTagBothThenFuse(s1, s2);
    }
    @Test
    public void testTagBothThenFuseSink() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testTagBothThenFuse(s1.print(), s2.print());
    }
    @Test
    public void testTagBothThenFuseSinkStream() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testTagBothThenFuse(s1.print(), s2);
    }
    
    private void testTagBothThenFuse(Placeable<?> s1, Placeable<?> s2)  {

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
    public void testFuseThenTagStream() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testFuseThenTag(s1, s2);
    }
    @Test
    public void testFuseThenTagSink() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testFuseThenTag(s1.print(), s2.print());
    }
    @Test
    public void testFuseThenTagStreamSink() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testFuseThenTag(s1, s2.print());
    }
    
    private void testFuseThenTag(Placeable<?> s1, Placeable<?> s2) {
        
        assertTrue(s1.getResourceTags().isEmpty());
        assertTrue(s2.getResourceTags().isEmpty());

        s1.fuse(s2);
        assertTrue(s1.getResourceTags().isEmpty());
        assertTrue(s2.getResourceTags().isEmpty());
        
        assertSame(s1.addResourceTags("ingest"), s1);
        assertSame(s2.addResourceTags("database"), s2);
     
        Set<String> expected = new HashSet<>();
        expected.add("ingest");
        expected.add("database");
        
        assertEquals(expected, s1.getResourceTags());
        assertEquals(s1.getResourceTags(), s2.getResourceTags()); 
    }
    
    @Test
    public void testFusing() {
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        TStream<String> snf = t.strings("3");
        
        assertSame(s1.fuse(s2), s1);
                
                
        String id1 = getFusingId(s1);
        String id2 = getFusingId(s2);
        
        assertNotNull(id1);
        assertFalse(id1.isEmpty());
        
        assertEquals(id1, id2);
        
        TStream<String> s3 = t.strings("3");
        TStream<String> s4 = t.strings("3");
        TSink s5 = s4.print();
        
        assertSame(s3.fuse(s4, s5), s3);
        assertEquals(getFusingId(s3), getFusingId(s4));
        assertEquals(getFusingId(s3), getFusingId(s5.operator()));
        
        assertFalse(getFusingId(s1).equals(getFusingId(s3)));
        
        assertNull(getFusingId(snf));
        
        TStream<String> s6 = StringStreams.toString(s4);
        s1.fuse(s6);
        assertEquals(getFusingId(s1), getFusingId(s6));
    }
    
    private static String getFusingId(TStream<?> s) {
        BOperator bop  =  ((BOutputPort) s.output()).operator();
        return getFusingId(bop);
    }
    
    private static String getFusingId(BOperator bop) {
        JSONObject fusing = ((JSONObject) bop.getConfig("fusing"));
        if (fusing == null)
            return null;
        Object ido = fusing.get("id");
        if (ido == null)
            return null;
        return ido.toString();
    }
    
}
