/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

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

public class TopologyTest {

    public static void assertFlowElement(Topology f, TopologyElement fe) {
        assertSame(f, fe.topology());
        assertSame(f.graph(), fe.graph());
    }

    @Test
    public void testBasics() {
        final Topology f = new Topology("F123");
        assertEquals("F123", f.getName());
        assertSame(f, f.topology());
        assertNotNull(f.graph());
    }
    
    @Test
    public void testDefaultName() {
        final Topology f = new Topology();
        assertSame(f, f.topology());
        assertEquals("testDefaultName", f.getName());
    }
    
    /**
     * When the calling method is 'main' we
     * use the class name.
     */
    @Test
    public void main() {
        final Topology f = new Topology();
        assertSame(f, f.topology());
        assertEquals("TopologyTest", f.getName());
    }
    

    @Test
    public void testStringStreamPrint() throws Exception {
        final Topology f = new Topology("Simple");
        TStream<String> source = f.strings("a", "b", "c");
        assertNotNull(source);
        source.print();
        checkPrint(f, "a", "b", "c");
    }

    public static void checkPrint(Topology f, String... strings)
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
