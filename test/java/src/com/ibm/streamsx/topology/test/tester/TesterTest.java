/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.tester;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.test.api.StreamTest;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class TesterTest extends TestTopology {
    @Test
    public void testStringFilter() throws Exception {
        final Topology topology = new Topology("StringFilter");
        TStream<String> source = topology.strings("hello", "goodbye",
                "farewell");
        StreamTest.assertStream(topology, source);

        TStream<String> filtered = source.filter(StreamTest.lengthFilter(5));

        Condition<Long> tupleCount = topology.getTester().tupleCount(filtered, 2);
        Condition<List<String>> contents = topology.getTester().stringContents(filtered,
                "goodbye", "farewell");

        complete(topology.getTester(), tupleCount, 10, TimeUnit.SECONDS);

        assertTrue(tupleCount.valid());
        assertTrue(contents.valid());
    }
    
    @Test
    public void testComplete1() throws Exception {
        assumeSPLOk();
        assumeTrue(getTesterType() == StreamsContext.Type.STANDALONE_TESTER);
        
        final Topology topology = new Topology("StringFilter");
        TStream<String> source = topology.strings("hello", "tester");

        final Tester tester = topology.getTester();
        Condition<Long> tupleCount = tester.tupleCount(source, 2);
        
        tester.complete(standalone());

        assertTrue(tupleCount.valid());
        
        try {
            tester.complete(standalone());
            fail("tester can be used twice");
        } catch (IllegalStateException e) { /* expected */ }
    }
    @Test
    public void testComplete2() throws Exception {
        assumeSPLOk();
        assumeTrue(getTesterType() == StreamsContext.Type.STANDALONE_TESTER);
        
        final Topology topology = new Topology("StringFilter");
        TStream<String> source = topology.strings("hello", "tester");

        final Tester tester = topology.getTester();
        Condition<Long> tupleCount = tester.tupleCount(source, 2);
        
        tester.complete(standalone(), tupleCount, 30, SECONDS);

        assertTrue(tupleCount.valid());
        
        try {
            tester.complete(standalone(), tupleCount, 30, SECONDS);
            fail("tester can be used twice");
        } catch (IllegalStateException e) { /* expected */ }
    }
    @Test
    public void testComplete3() throws Exception {
        assumeSPLOk();
        assumeTrue(getTesterType() == StreamsContext.Type.STANDALONE_TESTER);
        
        final Topology topology = new Topology("StringFilter");
        TStream<String> source = topology.strings("hello", "tester");

        final Tester tester = topology.getTester();
        Condition<Long> tupleCount = tester.tupleCount(source, 2);
        
        tester.complete(standalone(), new HashMap<>(), tupleCount, 30, SECONDS);
        
        assertTrue(tupleCount.valid());
        
        try {
            tester.complete(standalone(), new HashMap<>(), tupleCount, 30, SECONDS);
            fail("tester can be used twice");
        } catch (IllegalStateException e) { /* expected */ }
    }
    
    private StreamsContext<?> standalone() {
        return StreamsContextFactory.getStreamsContext(StreamsContext.Type.STANDALONE_TESTER);
    }
}
