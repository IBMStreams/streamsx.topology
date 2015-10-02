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

import org.junit.Ignore;
import org.junit.Test;

import static com.ibm.streamsx.topology.test.api.IsolateTest.getContainerIdAppend;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BOperator;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.builder.JOperator;
import com.ibm.streamsx.topology.builder.JOperator.JOperatorConfig;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.Placeable;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

/**
 * Tests to verify Placeable
 *
 */
public class PlaceableTest extends TestTopology {  

    @Test
    public void testSimpleTagsStream() {
        assumeTrue(isMainRun());
        Topology t = new Topology();        
        TStream<String> s = t.strings("3");
        testSimpleTags(s);
    }
    
    @Test
    public void testSimpleTagsSink() {
        assumeTrue(isMainRun());
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
        assumeTrue(isMainRun());
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testTagThenFuse(s1, s2);
    }
    
    @Test
    public void testTagThenFuseSink() {
        assumeTrue(isMainRun());
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testTagThenFuse(s1.print(), s2.print());
    }
    
    @Test
    public void testTagThenFuseStreamSink() {
        assumeTrue(isMainRun());
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
        assumeTrue(isMainRun());
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testTagBothThenFuse(s1, s2);
    }
    @Test
    public void testTagBothThenFuseSink() {
        assumeTrue(isMainRun());
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testTagBothThenFuse(s1.print(), s2.print());
    }
    @Test
    public void testTagBothThenFuseSinkStream() {
        assumeTrue(isMainRun());
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
        assumeTrue(isMainRun());
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testFuseThenTag(s1, s2);
    }
    @Test
    public void testFuseThenTagSink() {
        assumeTrue(isMainRun());
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testFuseThenTag(s1.print(), s2.print());
    }
    @Test
    public void testFuseThenTagStreamSink() {
        assumeTrue(isMainRun());
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
        assumeTrue(isMainRun());
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
        assumeTrue(isMainRun());
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
        JSONObject placement = JOperatorConfig.getJSONItem(bop.json(), JOperatorConfig.PLACEMENT);
        if (placement == null)
            return null;
        Object ido = placement.get(JOperator.PLACEMENT_EXPLICIT_COLOCATE_ID);
        if (ido == null)
            return null;
        return ido.toString();
    }
    
    private static Set<String> getResourceTags(TStream<?> s) {
        BOperator bop  =  ((BOutputPort) s.output()).operator();
        return getResourceTags(bop);
    }
    
    private static Set<String> getResourceTags(BOperator bop) {
        JSONObject placement = JOperatorConfig.getJSONItem(bop.json(), JOperatorConfig.PLACEMENT);
        if (placement == null)
            return null;
        JSONArray jat = (JSONArray) placement.get(JOperator.PLACEMENT_RESOURCE_TAGS);
        if (jat == null)
            return null;
        
        Set<String> tags = new HashSet<>();
        
        for (Object rt : jat)
            tags.add(rt.toString());
        
        return tags;
    }
    
    @Test
    public void testTags() {
        assumeTrue(isMainRun());
        Topology t = new Topology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        TStream<String> s3 = t.strings("3");
        
        s1.addResourceTags();
        assertNull(getResourceTags(s1));
        
        s2.addResourceTags("A", "B");
        Set<String> s2s = getResourceTags(s2);
        assertEquals(2, s2s.size());
        assertTrue(s2s.contains("A"));
        assertTrue(s2s.contains("B"));
        
        
        s3.addResourceTags("C", "D", "E");
        Set<String> s3s = getResourceTags(s3);
        assertEquals(3, s3s.size());
        assertTrue(s3s.contains("C"));
        assertTrue(s3s.contains("D"));
        assertTrue(s3s.contains("E"));
        
        s2s = getResourceTags(s2);
        assertEquals(2, s2s.size());
        assertTrue(s2s.contains("A"));
        assertTrue(s2s.contains("B"));

        s2.addResourceTags("X", "Y");
        s2s = getResourceTags(s2);
        assertEquals(4, s2s.size());
        assertTrue(s2s.contains("A"));
        assertTrue(s2s.contains("B"));
        assertTrue(s2s.contains("X"));
        assertTrue(s2s.contains("Y"));
        
        // Colocating means the s1 will inherit
        // s3 resource tags
        s1.colocate(s3);
        Set<String> s1s = getResourceTags(s1);
        assertEquals(3, s1s.size());
        assertTrue(s1s.contains("C"));
        assertTrue(s1s.contains("D"));
        assertTrue(s1s.contains("E"));       
    }
    
    /**
     * Test with a distributed execution with explicit
     * colocation of two functions end up on the same container.
     */
    @Test
    public void testSimpleDistributedColocate() throws Exception {
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
        
        Condition<List<String>> pes = t.getTester().stringContents(sa, "");
        
        Condition<Long> tc = t.getTester().tupleCount(sa, 2);
        
        complete(t.getTester(), tc, 10, TimeUnit.SECONDS);
        
        Set<String> singlePe = new HashSet<>(pes.getResult());
     
        assertTrue(pes.getResult().toString(), singlePe.size() == 1);
    }
    
    /**
     * Test with a distributed execution with explicit
     * colocation of two functions end up on the same container.
     */
    @Test
    @Ignore("Need to figure out how to get the tags set by the test") // TODO
    public void testSimpleDistributedHostTags() throws Exception {
        assumeTrue(SC_OK);
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);
        
        Topology t = new Topology();
        
        TStream<String> sa = t.strings("a");
       
        
        sa.addResourceTags("application");
        
        sa = sa.filter(new AllowAll<String>());
        sa.addResourceTags("application");
                
        getConfig().put(ContextProperties.KEEP_ARTIFACTS, Boolean.TRUE);
        
        Condition<List<String>> aout = t.getTester().stringContents(sa, "a");
        
        complete(t.getTester(), aout, 10, TimeUnit.SECONDS);
        assertTrue(aout.getResult().toString(), aout.valid());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testColocateLowLatancyNotPlaceable1() throws Exception {
        assumeTrue(isMainRun());
        
        // test current behavior of a not-placeable construct
        
        Topology t = new Topology("testColocateLowLatancyNotPlaceable1");
        
        TStream<String> s1 = 
                t.strings("a")
                .modify(getContainerIdAppend())
                .modify(getContainerIdAppend())
                ;
        
        @SuppressWarnings("unused")
        TStream<String> s2 = 
                t.strings("A")
                .lowLatency()
                .colocate(s1)  // throws IAE: not placeable
                .modify(getContainerIdAppend())
                .modify(getContainerIdAppend())
                .endLowLatency()
                ;
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testColocateLowLatancyNotPlaceable2() throws Exception {
        assumeTrue(isMainRun());
        
        // test current behavior of a not-placeable construct
        
        Topology t = new Topology("testColocateLowLatancyNotPlaceable2");
        
        TStream<String> s1 = 
                t.strings("a")
                .modify(getContainerIdAppend())
                .modify(getContainerIdAppend())
                ;
        
        @SuppressWarnings("unused")
        TStream<String> s2 = 
                t.strings("A")
                .lowLatency()
                .modify(getContainerIdAppend())
                .modify(getContainerIdAppend())
                .endLowLatency()
                .colocate(s1)  // throws IAE: not placeable
                ;
    }

    @Test(expected = IllegalStateException.class)
    @SuppressWarnings("unused")
    public void testColocateLowLatancy() throws Exception {
        assumeTrue(isMainRun());
        
        // test colocate doesn't violate low latency as well as does colocate
        
        final Topology topology = new Topology("testColocateLowLatancy");
        Tester tester = topology.getTester();
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        
        TStream<String> s1 = 
                topology.strings("a")
                .modify(getContainerIdAppend())
                ;
        
        TStream<String> s2 = 
                topology.strings("A")
                .lowLatency()
                .modify(getContainerIdAppend())
                .colocate(s1)  // expect throw ISE: colocate in a low latency region
                .modify(getContainerIdAppend())
                .endLowLatency()
                ;
        
        // once it's supported... (today it breaks the low latency guarantee)
        // and adjust isMainRun() too
//        // Given the default fuse-island behavior, expect islands to continue
//        // to be fused, now both in a single container.
//        
//        TStream<String> all = s1.union(s2);
//        all.print();
//        
//        Condition<Long> nTuples = tester.tupleCount(
//                                    all.filter(new AllowAll<String>()), 2);
//        Condition<List<String>> contents = tester.stringContents(
//                                    all.filter(new AllowAll<String>()), "");
//
//        complete(tester, nTuples, 10, TimeUnit.SECONDS);
//
//        Set<String> ids = getContainerIds(contents.getResult());
//        assertEquals("ids: "+ids, 1, ids.size());
    }
    
    @SuppressWarnings("unused")
    @Test(expected = IllegalStateException.class)
    public void testColocateLowLatencyRegions() throws Exception {
        assumeTrue(isMainRun());

        // ensure colocating two low latency regions doesn't break lowLatancy
        // and colocating is achieved.
        
        Topology t = new Topology("testColocateLowLatencyRegions");
        Tester tester = t.getTester();
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        
        TStream<String> s1 = 
                t.strings("a")
                .lowLatency()
                .modify(getContainerIdAppend())
                .modify(getContainerIdAppend())
                ;
                s1
                .endLowLatency()
                ;
        
        TStream<String> s2 = 
                t.strings("A")
                .lowLatency()
                .modify(getContainerIdAppend())
                .modify(getContainerIdAppend())
                ;
                s2
                .endLowLatency()
                ;
                
        s1.colocate(s2);  // expect throw ISE: colocate in a low latency region
        
        // once it's supported... (today it breaks the low latency guarantee)
        // and adjust isMainRun() too
//        // Given the default fuse-island behavior, expect islands to continue
//        // to be fused, now both in a single container.
//        
//        // Today FAILING in an interesting way.
//        // There are 2 PEs:
//        // - one has just the single colocated s1 and s2 modify ops
//        // - the other has everything else
//        
//        TStream<String> all = s1.union(s2);
//        all.print();
//        Condition<Long> nTuples = tester.tupleCount(all.filter(new AllowAll<String>()), 2);
//        Condition<List<String>> contents = tester.stringContents(
//                all.filter(new AllowAll<String>()), "");
//
//        complete(tester, nTuples, 10, TimeUnit.SECONDS);
//
//        Set<String> ids = getContainerIds(contents.getResult());
//        assertEquals("ids: "+ids, 1, ids.size());
    }    
    
    @Test(expected = IllegalStateException.class)
    public void testColocateIsolateViolation() throws Exception {
        assumeTrue(isMainRun());
       
        // verify s1.isolate().modify().colocate(s1) is disallowed
        
        Topology t = new Topology("testColocateIsolateViolation");
        
        TStream<String> s1 = t.strings("a");
        s1.isolate()
            .modify(getContainerIdAppend())
            .colocate(s1)  // throws ISE: can't colocate isolated stream with parent
            ;
    }

}
