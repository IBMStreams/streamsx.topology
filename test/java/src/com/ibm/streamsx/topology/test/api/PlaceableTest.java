/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static com.ibm.streamsx.topology.logic.Logic.identity;
import static com.ibm.streamsx.topology.test.api.IsolateTest.getContainerIdAppend;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Ignore;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.Placeable;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
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
        Topology t = newTopology();        
        TStream<String> s = t.strings("3");
        testSimpleTags(s);
    }
    
    @Test
    public void testSimpleTagsSink() {
        assumeTrue(isMainRun());
        Topology t = newTopology();        
        TStream<String> s = t.strings("3");
        testSimpleTags(s.print());
    }
    
    private void testSimpleTags(Placeable<?> s) {
        
        assertNotNull(s.getInvocationName());
        
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
    public void testTagThenFuseStream() throws Exception {
        adlOk();
        
        Topology t = newTopology();        
        TStream<String> s1 = t.strings("3").invocationName("S1");
        TStream<String> s2 = t.strings("3").invocationName("S2");
        testTagThenFuse(s1, s2);
        
        Document adl = produceADL(t);
        adlAssertColocated(adl, false, "S1", "S2");
    }
    
    @Test
    public void testTagThenFuseSink() throws Exception {
        adlOk();
        
        Topology t = newTopology();        
        TStream<String> s1 = t.strings("3").invocationName("S1");
        TStream<String> s2 = t.strings("3").invocationName("S2");
        testTagThenFuse(s1.print().invocationName("S1P"), s2.print().invocationName("S2P"));
        
        Document adl = produceADL(t);
        adlAssertColocated(adl, false, "S1P", "S2P");
    }
    
    @Test
    public void testTagThenFuseStreamSink() throws Exception {
        adlOk();
        
        Topology t = newTopology();        
        TStream<String> s1 = t.strings("3").invocationName("S1");
        TStream<String> s2 = t.strings("3").invocationName("S2");
        testTagThenFuse(s1, s2.print().invocationName("S2P"));
        
        Document adl = produceADL(t);
        adlAssertColocated(adl, false, "S1", "S2P");
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
    public void testTagBothThenFuseStream() throws Exception {
        adlOk();
        
        Topology t = newTopology();        
        TStream<String> s1 = t.strings("3").invocationName("S1");
        TStream<String> s2 = t.strings("3").invocationName("S2");
        testTagBothThenFuse(s1, s2);
        
        Document adl = produceADL(t);
        adlAssertColocated(adl, false, "S1", "S2");
    }
    @Test
    public void testTagBothThenFuseSink() throws Exception {
        adlOk();
        
        Topology t = newTopology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testTagBothThenFuse(s1.print().invocationName("S1P"), s2.print().invocationName("S2P"));
        
        Document adl = produceADL(t);
        adlAssertColocated(adl, false, "S1P", "S2P");
    }
    @Test
    public void testTagBothThenFuseSinkStream() throws Exception {
        adlOk();
        
        Topology t = newTopology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3").invocationName("S2");
        testTagBothThenFuse(s1.print().invocationName("S1P"), s2);
        
        Document adl = produceADL(t);
        adlAssertColocated(adl, false, "S1P", "S2");
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
    public void testFuseThenTagStream() throws Exception {
        adlOk();
        
        Topology t = newTopology();        
        TStream<String> s1 = t.strings("3").invocationName("S1");
        TStream<String> s2 = t.strings("3").invocationName("S2");
        testFuseThenTag(s1, s2);
        
        Document adl = produceADL(t);
        adlAssertColocated(adl, false, "S1", "S2");
    }
    @Test
    public void testFuseThenTagSink() throws Exception {
        adlOk();
        
        Topology t = newTopology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        testFuseThenTag(s1.print().invocationName("S1P"), s2.print().invocationName("S2P"));
        
        Document adl = produceADL(t);
        adlAssertColocated(adl, false, "S1P", "S2P");
    }
    @Test
    public void testFuseThenTagStreamSink() throws Exception {
        adlOk();
        
        Topology t = newTopology();        
        TStream<String> s1 = t.strings("3").invocationName("S1");
        TStream<String> s2 = t.strings("3");
        testFuseThenTag(s1, s2.print().invocationName("S2P"));
        
        Document adl = produceADL(t);
        adlAssertColocated(adl, false, "S1", "S2P");
    }
    
    @Test
    public void testSplit() {
        assumeTrue(isMainRun());
        Topology t = newTopology();        
        TStream<String> s = t.strings("3");
        List<TStream<String>> splits = s.split(3, x -> 0);
        
        splits.get(0).addResourceTags("tag1");
        for (TStream<String> l : splits) {
            assertEquals(1, l.getResourceTags().size());
            assertTrue(l.getResourceTags().contains("tag1"));
        } 
        splits.get(2).addResourceTags("tag921");
        for (TStream<String> l : splits) {
            assertEquals(2, l.getResourceTags().size());
            assertTrue(l.getResourceTags().contains("tag1"));
            assertTrue(l.getResourceTags().contains("tag921"));
        } 
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
    public void testFusing() throws Exception {
        adlOk();
        
        Topology t = newTopology();        
        TStream<String> s1 = t.strings("3").invocationName("S1");
        TStream<String> s2 = t.strings("3").invocationName("S2");
        TStream<String> snf = t.strings("3");
        
        assertTrue(s1.isPlaceable());
        
        assertSame(s1.colocate(s2), s1);
        
        TStream<String> s3 = t.strings("3").invocationName("S3");
        TStream<String> s4 = t.strings("3").invocationName("S4");
        TSink s5 = s4.print().invocationName("S5");
        assertTrue(s5.isPlaceable());
        
        assertSame(s3.colocate(s4, s5), s3);
        
        TStream<String> s6 = StringStreams.toString(s4).invocationName("S6");
        s1.colocate(s6);
        
        Document adl = produceADL(s6);
        adlAssertDefaultHostpool(adl);
        adlAssertColocated(adl, false, "S1", "S2", "S6");
        adlAssertColocated(adl, false, "S3", "S4", "S5");
    }
    
    @Test
    public void testNonplaceable() {
        assumeTrue(isMainRun());
        Topology t = newTopology();
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        
        TStream<String> su = s1.union(s2);
        assertFalse(su.isPlaceable());
        assertFalse(s1.isolate().isPlaceable());
        
        assertNull(su.getInvocationName());
        
        TStream<String> sp = s1.parallel(3);
        assertFalse(sp.isPlaceable());
        assertFalse(sp.endParallel().isPlaceable());
    }
    
    @Test
    public void testTags() {
        assumeTrue(isMainRun());
        Topology t = newTopology();        
        TStream<String> s1 = t.strings("3");
        TStream<String> s2 = t.strings("3");
        TStream<String> s3 = t.strings("3");
        
        s1.addResourceTags();
        assertTrue(s1.getResourceTags().isEmpty());
        
        s2.addResourceTags("A", "B");
        Set<String> s2s = s2.getResourceTags();
        assertEquals(2, s2s.size());
        assertTrue(s2s.contains("A"));
        assertTrue(s2s.contains("B"));
        
        
        s3.addResourceTags("C", "D", "E");
        Set<String> s3s = s3.getResourceTags();
        assertEquals(3, s3s.size());
        assertTrue(s3s.contains("C"));
        assertTrue(s3s.contains("D"));
        assertTrue(s3s.contains("E"));
        
        s2s = s2.getResourceTags();
        assertEquals(2, s2s.size());
        assertTrue(s2s.contains("A"));
        assertTrue(s2s.contains("B"));

        s2.addResourceTags("X", "Y");
        s2s = s2.getResourceTags();
        assertEquals(4, s2s.size());
        assertTrue(s2s.contains("A"));
        assertTrue(s2s.contains("B"));
        assertTrue(s2s.contains("X"));
        assertTrue(s2s.contains("Y"));
        
        // Colocating means the s1 will inherit
        // s3 resource tags
        s1.colocate(s3);
        Set<String> s1s = s1.getResourceTags();
        assertEquals(3, s1s.size());
        assertTrue(s1s.contains("C"));
        assertTrue(s1s.contains("D"));
        assertTrue(s1s.contains("E"));       
    }
    
    /**
     *
     */
    @Test
    public void testSimpleColocate() throws Exception {
        adlOk();
        
        Topology t = newTopology();
        
        TStream<String> sa = t.strings("a");
        TStream<String> sb = t.strings("b");
        
        sa = sa.transform(tuple->tuple).invocationName("SA");
        sb = sb.transform(tuple->tuple).invocationName("SB");
        
        sa.colocate(sb);
                
        sa = sa.isolate().filter(tuple->true);
        sb = sb.isolate().filter(tuple->true);
        
        sa = sa.union(sb);
        
        Document adl = produceADL(t);
        adlAssertDefaultHostpool(adl);
        adlAssertColocated(adl, false, "SA", "SB");
    }
    
    @Test
    public void testNoColocate() throws Exception {
        adlOk();
        
        Topology t = newTopology();
        
        TStream<String> sa = t.strings("a");
        TStream<String> sb = t.strings("b");
        
        sa = sa.transform(identity());
        sb = sb.transform(tuple->tuple);
       
        sa = sa.union(sb);
        
        sa.forEach(tuple->{});
        
        Document adl = produceADL(t);
        adlAssertDefaultHostpool(adl);
        adlAssertNoColocated(adl);
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
        
        Topology t = newTopology();
        
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
        
        Topology t = newTopology("testColocateLowLatancyNotPlaceable1");
        
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
        
        Topology t = newTopology("testColocateLowLatancyNotPlaceable2");
        
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
        
        final Topology topology = newTopology("testColocateLowLatancy");
        Tester tester = topology.getTester();
        
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
        
        Topology t = newTopology("testColocateLowLatencyRegions");
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
        
        Topology t = newTopology("testColocateIsolateViolation");
        
        TStream<String> s1 = t.strings("a");
        s1.isolate()
            .modify(getContainerIdAppend())
            .colocate(s1)  // throws ISE: can't colocate isolated stream with parent
            ;
    }

    public static Document produceADL(TopologyElement te) throws Exception {
        @SuppressWarnings("unchecked")
        StreamsContext<File> ctx = (StreamsContext<File>) StreamsContextFactory.getStreamsContext(StreamsContext.Type.TOOLKIT);
        File tkDir = ctx.submit(te.topology()).get();
        
        System.out.println("TKDIR:" + tkDir);
        
       
        String topoTkDir = System.getProperty("topology.toolkit.release");
        
        List<String> cmd = new ArrayList<>();
        cmd.add(System.getenv("STREAMS_INSTALL") + "/bin/sc");    
        cmd.add("--suppress-all-but-the-application-model");
        cmd.add("--spl-path");
        cmd.add(topoTkDir);
        cmd.add("--main-composite");
        cmd.add(te.topology().getNamespace() + "::" + te.topology().getName());
        
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.directory(tkDir);
        pb.redirectInput(Redirect.INHERIT);
        pb.redirectOutput(Redirect.INHERIT);
        pb.redirectError(Redirect.INHERIT);
        
        Process p = pb.start();
        int rc = p.waitFor();
        System.out.println("RC:" + rc);
        assertEquals(0, rc);
        
        File adl = new File(tkDir, "output/" + te.topology().getNamespace() + "." + te.topology().getName() + ".adl");
        assertTrue(adl.exists());
        assertTrue(adl.isFile());
        
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        
        DocumentBuilder db = dbf.newDocumentBuilder(); 
        
        
        try(InputStream adlIn = new FileInputStream(adl)) {

        return db.parse(new InputSource(adlIn));
        }
    }
    
    private static String attr(Node node, String name) {
        String value = node.getAttributes().getNamedItem(name).getTextContent();
        assertNotNull(value);
        return value;
    }
    
    public static void adlAssertDefaultHostpool(Document adl) {
        
        NodeList pools = adl.getElementsByTagName("hostpool");
        assertEquals(1, pools.getLength());
        assertEquals("$default", attr(pools.item(0), "name")); 
    }
    
    // <coLocation colocId="__jaa_colocate1"/>;
    public static String colocateId(Node op) {
        Element ope = (Element) op;
        NodeList colocates = ope.getElementsByTagName("coLocation");
        if (colocates.getLength() == 0)
            return null;
        assertEquals(1, colocates.getLength());
        return attr(colocates.item(0), "colocId");
    }
    
    /**
     * Assert that all named operators are colocated and
     * that no others are colocated with them.
     */
    public static void adlAssertColocated(Document adl, boolean channel, String ...names) {
        
        Set<String> cnames = new HashSet<>(Arrays.asList(names));
        assertEquals(names.length, cnames.size());
        
        List<Node> colocated = new ArrayList<>();
        List<Node> notcolocated = new ArrayList<>();
        
        NodeList ops = adl.getElementsByTagName("primitiveOperInstance");
        assertTrue(ops.getLength() >= names.length);
        for (int i = 0; i < ops.getLength(); i++) {
            
            Node op = ops.item(i);
            assertEquals(Node.ELEMENT_NODE, op.getNodeType());
            String opName = attr(op, "name");
            if (opName.indexOf('.') != -1)
                opName = opName.substring(opName.lastIndexOf('.')+1);
            if (cnames.contains(opName))
                colocated.add(op);
            else
                notcolocated.add(op);
        }
        assertEquals(names.length, colocated.size());
        
        final String colocateId = colocateId(colocated.get(0));
        assertNotNull(colocateId);
        
        for (Node op : colocated)
            assertEquals(colocateId, colocateId(op));

        for (Node op : notcolocated)
            assertFalse(colocateId.equals(colocateId(op)));
        
        if (channel)
            assertTrue(colocateId, colocateId.contains("getChannel()"));
        else
            assertFalse(colocateId, colocateId.contains("getChannel()"));
    }
    /**
     * Assert that no operators are colocated.
     */
    public static void adlAssertNoColocated(Document adl) {
               
        NodeList ops = adl.getElementsByTagName("primitiveOperInstance");
        for (int i = 0; i < ops.getLength(); i++) {
            Node op = ops.item(i);
            assertEquals(Node.ELEMENT_NODE, op.getNodeType());
            assertNull(colocateId(op));
        }
    }
}
