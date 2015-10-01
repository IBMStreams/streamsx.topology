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

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.test.TestTopology;

public class TopologyTest extends TestTopology {

    public static void assertFlowElement(Topology f, TopologyElement fe) {
        assertSame(f, fe.topology());
        assertSame(f.graph(), fe.graph());
    }

    @Test
    public void testBasics() {
        assumeTrue(isMainRun());
        final Topology f = new Topology("F123");
        assertEquals("F123", f.getName());
        assertSame(f, f.topology());
        assertNotNull(f.graph());
    }
    
    @Test
    public void testDefaultName() {
        assumeTrue(isMainRun());
        final Topology f = new Topology();
        assertSame(f, f.topology());
        assertEquals("testDefaultName", f.getName());
    }
    
    /**
     * Test that we fail when an anonymous class
     * captures a non-static reference.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testNonStaticContext() {
        assumeTrue(isMainRun());
        final Topology t = new Topology();
        
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
        final Topology f = new Topology("Simple");
        TStream<String> source = f.strings("a", "b", "c");
        assertNotNull(source);
        source.print();
        checkPrintEmbedded(f, "a", "b", "c");
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
