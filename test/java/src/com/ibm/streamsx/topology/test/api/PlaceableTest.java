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
import static org.junit.Assume.assumeTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BOperator;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.builder.json.JOperator;
import com.ibm.streamsx.topology.builder.json.JOperator.JOperatorConfig;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.Placeable;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * Tests to verify Placeable
 *
 */
public class PlaceableTest extends TestTopology {  

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
        s1.colocate(s2);
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
        s1.colocate(s2);
        
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

        s1.colocate(s2);
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
        
        assertTrue(s1.isPlaceable());
        
        assertSame(s1.colocate(s2), s1);
                
                
        String id1 = getFusingId(s1);
        String id2 = getFusingId(s2);
        
        assertNotNull(id1);
        assertFalse(id1.isEmpty());
        
        assertEquals(id1, id2);
        
        TStream<String> s3 = t.strings("3");
        TStream<String> s4 = t.strings("3");
        TSink s5 = s4.print();
        assertTrue(s5.isPlaceable());
        
        assertSame(s3.colocate(s4, s5), s3);
        assertEquals(getFusingId(s3), getFusingId(s4));
        assertEquals(getFusingId(s3), getColocate(s5.operator()));
        
        assertFalse(getFusingId(s1).equals(getFusingId(s3)));
        
        assertNull(getFusingId(snf));
        
        TStream<String> s6 = StringStreams.toString(s4);
        s1.colocate(s6);
        assertEquals(getFusingId(s1), getFusingId(s6));
    }
    
    @Test
    public void testNonplaceable() {
        Topology t = new Topology();
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        
        assertFalse(s1.union(s2).isPlaceable());
        assertFalse(s1.isolate().isPlaceable());
        
        TStream<String> sp = s1.parallel(3);
        assertFalse(sp.isPlaceable());
        assertFalse(sp.endParallel().isPlaceable());
    }
    
    private static String getFusingId(TStream<?> s) {
        BOperator bop  =  ((BOutputPort) s.output()).operator();
        return getColocate(bop);
    }
    
    private static String getColocate(BOperator bop) {
        JSONObject fusing = JOperatorConfig.getJSONItem(bop.json(), JOperatorConfig.PLACEMENT);
        if (fusing == null)
            return null;
        Object ido = fusing.get(JOperator.PLACEMENT_EXPLICIT_COLOCATE_ID);
        if (ido == null)
            return null;
        return ido.toString();
    }
    
    /**
     * Test with a distributed execution with explicit
     * colocation of two functions end up on the same container.
     */
    @Test
    public void testSimpleDistributed() throws Exception {
        assumeTrue(SC_OK);
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);
        
        Topology t = new Topology();
        
        TStream<String> sa = t.strings("a");
        TStream<String> sb = t.strings("b");
        
        sa = sa.transform(IsolateTest.getContainerId());
        sb = sb.transform(IsolateTest.getContainerId());
        
        sa.colocate(sb);
                
        sa = sa.isolate().filter(new AllowAll<String>());
        sb = sb.isolate().filter(new AllowAll<String>());
        
        sa = sa.union(sb);
        
        getConfig().put(ContextProperties.KEEP_ARTIFACTS, Boolean.TRUE);
        
        Condition<List<String>> pes = t.getTester().stringContents(sa, "");
        
        Condition<Long> tc = t.getTester().tupleCount(sa, 2);
        
        complete(t.getTester(), tc, 10, TimeUnit.SECONDS);
        
        Set<String> singlePe = new HashSet<>(pes.getResult());
     
        assertTrue(pes.getResult().toString(), singlePe.size() == 1);
    }
    
}
