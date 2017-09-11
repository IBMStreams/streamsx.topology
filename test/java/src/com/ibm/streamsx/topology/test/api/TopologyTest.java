/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assume.assumeTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;

public class TopologyTest extends TestTopology {

    public static void assertFlowElement(Topology f, TopologyElement fe) {
        assertSame(f, fe.topology());
    }

    @Test
    public void testBasics() {
        assumeTrue(isMainRun());
        final Topology f = new Topology("F123");
        assertEquals("F123", f.getName());
        assertEquals("com.ibm.streamsx.topology.test.api", f.getNamespace());
        assertSame(f, f.topology());
        
        final Topology f2 = new Topology("NS123", "F456");
        assertEquals("NS123", f2.getNamespace());
        assertEquals("F456", f2.getName());
        assertSame(f2, f2.topology());
    }
    
    @Test
    public void testDefaultName() {
        assumeTrue(isMainRun());
        final Topology f = new Topology();
        assertSame(f, f.topology());
        assertEquals("testDefaultName", f.getName());
        assertEquals("com.ibm.streamsx.topology.test.api", f.getNamespace());
    }
    
    /**
     * Test that we fail when an anonymous class
     * captures a non-static reference.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testNonStaticContext() {
        assumeTrue(isMainRun());
        final Topology t = newTopology();
        
        // This captures a reference to the instance
        // of TopologyTest running the test, which is
        // not serializable, thus it will fail.
        t.source(new Supplier<Iterable<String>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> get() {
                // TODO Auto-generated method stub
                return null;
            }});
    }
    
    /**
     * When the calling method is 'main' we
     * use the class name.
     */
    @Test
    public void main() {
        assumeTrue(isMainRun());
        final Topology f = new Topology();
        assertSame(f, f.topology());
        assertEquals("TopologyTest", f.getName());
    }
    

    @Test
    public void testStringStreamPrint() throws Exception {
        assumeTrue(isEmbedded());  // checkPrint() forces embedded context
        final Topology f = newTopology("Simple");
        TStream<String> source = f.strings("a", "b", "c");
        assertNotNull(source);
        source.print();
        checkPrintEmbedded(f, "a", "b", "c");
    }
    
    private static String largeString(String base, final int len) {
        StringBuilder sb = new StringBuilder(len);
        sb.append(base);
        while (sb.length() < len)
            sb.append("0123456789".substring(0, Math.min(10, len-sb.length())));
        assert len == sb.length();
        
        return sb.toString();
    }
    @Test
    public void testLongNames() throws Exception {
        // 255 is max file name size.
        // 4 is the length of the '.spl'
        String ns = largeString("myns", 255);
        String appname = largeString("myApp", 255 - 5);
        final Topology topo = newTopology(ns, appname);
        
        TStream<String> source = topo.strings("a", "b", "c");
        Condition<List<String>> ec = topo.getTester().stringContents(source, "a", "b", "c");
        complete(topo.getTester(), ec, 10, TimeUnit.SECONDS);
    }
    
    /**
     * Test a combination of namespace and name being larger than 255.
     */
    @Test
    public void testLongNamespaceAndNames() throws Exception {

        String ns = largeString("a", 50);
        for (int i = 0; i < 4; i++)
            ns = ns.concat(largeString(".b", 60));
        String appname = largeString("myApp", 40);
        final Topology topo = newTopology(ns, appname);
        
        TStream<String> source = topo.strings("a", "b", "c");
        Condition<List<String>> ec = topo.getTester().stringContents(source, "a", "b", "c");
        complete(topo.getTester(), ec, 10, TimeUnit.SECONDS);
    }
    @Test
    public void testBadChars() throws Exception {

        final Topology topo = newTopology("<>{}", "<>{}");
        
        TStream<String> source = topo.strings("a", "b", "c");
        Condition<List<String>> ec = topo.getTester().stringContents(source, "a", "b", "c");
        complete(topo.getTester(), ec, 10, TimeUnit.SECONDS);
    }
    
    @Test
    public void testUnicodeNames() throws Exception {

        final Topology topo = newTopology("com.树倒猢狲散.早起的鳥兒有蟲吃", "良药苦口");
        
        TStream<String> source = topo.strings("a", "b", "c");
        Condition<List<String>> ec = topo.getTester().stringContents(source, "a", "b", "c");
        complete(topo.getTester(), ec, 10, TimeUnit.SECONDS);
    }
    public static void checkPrintEmbedded(Topology f, String... strings)
            throws Exception {

        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream newOut = new PrintStream(baos);
        System.setOut(newOut);
        try {
            StreamsContextFactory.getEmbedded().submit(f).get();
        } finally {
            System.out.flush();
            System.setOut(originalOut);
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

        BufferedReader result = new BufferedReader(new InputStreamReader(bais));

        for (String s : strings)
            assertEquals(s, result.readLine());
        assertNull(result.readLine());
    }
}
