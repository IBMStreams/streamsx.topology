/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.embedded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.flow.handlers.StreamCollector;
import com.ibm.streams.flow.handlers.StreamCounter;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.spl.FileSPLStreams;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class SimpleEmbeddedTest extends TestTopology {

    @Before
    public void checkEmbedded() {
        assumeTrue(isEmbedded());
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
                .getStreamsContext(StreamsContext.Type.EMBEDDED_TESTER)
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
                .getStreamsContext(StreamsContext.Type.EMBEDDED_TESTER)
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
                .getStreamsContext(StreamsContext.Type.EMBEDDED_TESTER)
                .submit(topology).get();

        assertEquals(3, counter.getTupleCount());
        assertEquals("Hello", collector.getTuples().get(0).getString(0));
        assertEquals("World!", collector.getTuples().get(1).getString(0));
        assertEquals("Test!!", collector.getTuples().get(2).getString(0));

        assertEquals(2, counter2.getTupleCount());
        assertEquals("Hello", collector2.getTuples().get(0).getString(0));
        assertEquals("Test!!", collector2.getTuples().get(1).getString(0));
    }
    
    private Predicate<String> nilFilter = getNilFilter();
    private static Predicate<String> getNilFilter() {
        Predicate<String> nilFilter = new Predicate<String>() {
            private static final long serialVersionUID = 1L;
            public boolean test(String tuple) { return true; }
        };
        return nilFilter;
    }

    private static class AppendXform implements Function<String, String> {
        private static final long serialVersionUID = 1L;
        private final String s;
        public AppendXform(String s) { this.s = s; }
        public String apply(String tuple) {
            return tuple+s;
        }
    }

    @Test
    public void testIsSupported() throws Exception {

        Topology topology = new Topology("test");

        TStream<String> hw = topology.strings("Hello", "World!", "Test!!");
        TStream<String> hw2 = hw.transform(new AppendXform("(stream-2)"));
        // make sure "marker" ops are ok: union,parallel,unparallel
        hw
            .union(hw2)
            .parallel(2)
                .filter(nilFilter)
                .filter(nilFilter)
            .endParallel()
        .print();

        StreamsContext<?> sc = StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.EMBEDDED);
        assertTrue(sc.isSupported(topology));

        sc = StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.EMBEDDED_TESTER);
        assertTrue(sc.isSupported(topology));
    }

    @Test
    public void testIsSupportedNeg() throws Exception {
        
        StreamSchema AnySchema = Type.Factory
                .getStreamSchema("tuple<rstring ticker>");

        Topology topology = new Topology("test");

        TStream<String> hw = topology.strings("Hello", "World!", "Test!!");
        // add SPL (non-java) operator
        FileSPLStreams.csvReader(hw, AnySchema );

        StreamsContext<?> sc = StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.EMBEDDED);
        assertTrue(!sc.isSupported(topology));
        try {
            sc.submit(topology);
            fail("None supported was submitted");
        } catch (IllegalStateException e) { /* expected */ }

        sc = StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.EMBEDDED_TESTER);
        assertTrue(!sc.isSupported(topology));
        try {
            sc.submit(topology);
            fail("None supported was submitted");
        } catch (IllegalStateException e) { /* expected */ }
    }

}
