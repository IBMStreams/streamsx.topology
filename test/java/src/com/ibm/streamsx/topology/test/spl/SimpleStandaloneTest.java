/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.spl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.flow.handlers.StreamCollector;
import com.ibm.streams.flow.handlers.StreamCounter;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class SimpleStandaloneTest extends TestTopology {
    
    @Before
    public void checkStandalone() {
        assumeSPLOk();
        assumeTrue(getTesterType() == Type.STANDALONE_TESTER);
    }

    @Test
    public void testSimple() throws Exception {

        Topology topology = new Topology("testSimple");

        TStream<String> hw = topology.strings("Hello", "World!", "Test!!");
        SPLStream hws = SPLStreams.stringToSPLStream(hw);

        Tester tester = topology.getTester();
        StreamCounter<Tuple> counter = tester.splHandler(hws,
                new StreamCounter<Tuple>());
        StreamCollector<LinkedList<Tuple>, Tuple> collector = tester
                .splHandler(hws, StreamCollector.newLinkedListCollector());

        StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.STANDALONE_TESTER)
                .submit(topology).get();

        assertEquals(3, counter.getTupleCount());
        assertEquals("Hello", collector.getTuples().get(0).getString(0));
        assertEquals("World!", collector.getTuples().get(1).getString(0));
        assertEquals("Test!!", collector.getTuples().get(2).getString(0));
    }

    @Test
    public void testSimpleWithConditions() throws Exception {

        Topology topology = new Topology("testSimpleConditions");

        TStream<String> hw = topology.strings("Hello", "World!", "Test!!");

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(hw, 3);
        Condition<List<String>> expectedContents = tester.stringContents(hw,
                "Hello", "World!", "Test!!");

        StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.STANDALONE_TESTER)
                .submit(topology).get();

        assertTrue(expectedCount.valid());
        assertTrue(expectedContents.valid());
    }

    @Test
    public void testTwoStreams() throws Exception {

        Topology topology = new Topology("testTwoStreams");

        TStream<String> hw = topology.strings("Hello", "World!", "Test!!");
        SPLStream hws = SPLStreams.stringToSPLStream(hw);

        TStream<String> hw2 = StringStreams.contains(hw, "e");
        SPLStream hw2s = SPLStreams.stringToSPLStream(hw2);

        Tester tester = topology.getTester();
        StreamCounter<Tuple> counter = tester.splHandler(hws,
                new StreamCounter<Tuple>());
        StreamCollector<LinkedList<Tuple>, Tuple> collector = tester
                .splHandler(hws, StreamCollector.newLinkedListCollector());

        StreamCounter<Tuple> counter2 = tester.splHandler(hw2s,
                new StreamCounter<Tuple>());
        StreamCollector<LinkedList<Tuple>, Tuple> collector2 = tester
                .splHandler(hw2s, StreamCollector.newLinkedListCollector());

        StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.STANDALONE_TESTER)
                .submit(topology).get();

        assertEquals(3, counter.getTupleCount());
        assertEquals("Hello", collector.getTuples().get(0).getString(0));
        assertEquals("World!", collector.getTuples().get(1).getString(0));
        assertEquals("Test!!", collector.getTuples().get(2).getString(0));

        assertEquals(2, counter2.getTupleCount());
        assertEquals("Hello", collector2.getTuples().get(0).getString(0));
        assertEquals("Test!!", collector2.getTuples().get(1).getString(0));
    }

}
