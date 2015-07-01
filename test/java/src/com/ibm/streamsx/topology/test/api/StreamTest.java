/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.function7.Predicate;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class StreamTest extends TestTopology {

    public static void assertStream(Topology f, TStream<?> stream) {
        TopologyTest.assertFlowElement(f, stream);
    }

    @Test
    public void testBasics() throws Exception {
        final Topology topology = new Topology("BasicStream");
        
        assertEquals("BasicStream", topology.getName());
        assertSame(topology, topology.topology());
        
        TStream<String> source = topology.strings("a", "b", "c");
        assertStream(topology, source);
        assertSame(String.class, source.getTupleClass());
    }

    @Test
    public void testStringFilter() throws Exception {
        final Topology f = new Topology("StringFilter");
        TStream<String> source = f.strings("hello", "goodbye", "farewell");
        assertStream(f, source);

        TStream<String> filtered = source.filter(lengthFilter(5));
        
        completeAndValidate(filtered, 10, "goodbye", "farewell");
    }

    @SuppressWarnings("serial")
    public static Predicate<String> lengthFilter(final int length) {
        return new Predicate<String>() {

            @Override
            public boolean test(String v1) {
                return v1.length() > length;
            }
        };
    }

    @Test
    public void testTransform() throws Exception {
        final Topology f = new Topology("TransformStream");
        TStream<String> source = f.strings("325", "457", "9325");
        assertStream(f, source);

        TStream<Integer> i1 = source.transform(stringToInt(), Integer.class);
        TStream<Integer> i2 = i1.transform(add17(), Integer.class);
        completeAndValidate(i2, 10, "342", "474", "9342");
    }

    @Test
    public void testTransformWithDrop() throws Exception {
        final Topology f = new Topology("TransformStream");
        TStream<String> source = f.strings("93", "68", "221");
        assertStream(f, source);

        TStream<Integer> i1 = source.transform(stringToIntExcept68(),
                Integer.class);
        TStream<Integer> i2 = i1.transform(add17(), Integer.class);
        
        completeAndValidate(i2, 10, "110", "238");
    }

    @Test
    public void testMultiTransform() throws Exception {
        final Topology topology = new Topology("MultiTransformStream");
        TStream<String> source = topology.strings("mary had a little lamb",
                "its fleece was white as snow");
        assertStream(topology, source);

        TStream<String> words = source.multiTransform(splitWords(),
                String.class);
        
        completeAndValidate(words, 10, "mary", "had",
                "a", "little", "lamb", "its", "fleece", "was", "white", "as",
                "snow");
    }

    @Test
    public void testUnionNops() throws Exception {
        final Topology f = new Topology("Union");
        TStream<String> s1 = f.strings("A1", "B1", "C1", "D1");

        Set<TStream<String>> empty = Collections.emptySet();
        assertSame(s1, s1.union(s1));
        assertSame(s1, s1.union(empty));
        assertSame(s1, s1.union(Collections.singleton(s1)));
    }

    @Test
    public void testUnion() throws Exception {
        final Topology topology = new Topology("Union");
        TStream<String> s1 = topology.strings("A1", "B1", "C1", "D1");
        TStream<String> s2 = topology.strings("A2", "B2", "C2", "D2");

        assertNotSame(s1, s2);

        TStream<String> su = s1.union(s2);

        assertNotSame(su, s1);
        assertNotSame(su, s2);

        // TODO - testing doesn't work against union streams in embedded.
        su = su.filter(new AllowAll<String>());

        Tester tester = topology.getTester();

        Condition<Long> suCount = tester.tupleCount(su, 8);
        Condition<List<String>> suContents = tester.stringContentsUnordered(su, "A1", "B1", "C1", "D1", "A2", "B2", "C2", "D2");

        assertTrue(complete(tester, suCount, 10, TimeUnit.SECONDS));


        assertTrue("SU:" + suCount, suCount.valid());
        assertTrue("SU:" + suContents, suContents.valid());
    }

    @Test
    public void testUnionSet() throws Exception {
        final Topology topology = new Topology("Union");
        TStream<String> s1 = topology.strings("A1", "B1", "C1");
        TStream<String> s2 = topology.strings("A2", "B2", "C2", "D2");
        TStream<String> s3 = topology.strings("A3", "B3", "C3");
        Set<TStream<String>> streams = new HashSet<TStream<String>>();
        streams.add(s2);
        streams.add(s3);
        TStream<String> su = s1.union(streams);

        // TODO - testing doesn't work against union streams in embedded.
        su = su.filter(new AllowAll<String>());

        Tester tester = topology.getTester();

        Condition<Long> suCount = tester.tupleCount(su, 10);
        Condition<List<String>> suContents = tester.stringContentsUnordered(su, "A1", "B1",
                "C1", "A2", "B2", "C2", "D2", "A3", "B3", "C3");

        assertTrue(complete(tester, suCount, 10, TimeUnit.SECONDS));

        // assertTrue("SU:" + suContents, suContents.valid());
        assertTrue("SU:" + suCount, suCount.valid());
        assertTrue("SUContents:" + suContents, suContents.valid());
    }

    @Test
    public void testSimpleParallel() throws Exception {

        final Topology topology = new Topology("EmbeddedParallel");
        TStream<Number> s1 = topology.numbers(1, 2, 3, 94, 5, 6).parallel(6)
                .filter(new AllowAll<Number>()).unparallel();

        TStream<String> sp = StringStreams.toString(s1);

        Tester tester = topology.getTester();

        Condition<Long> spCount = tester.tupleCount(sp, 6);
        Condition<List<String>> spContents = tester.stringContentsUnordered(sp, "1", "2",
                "3", "94", "5", "6");

        complete(tester, spCount, 60, TimeUnit.SECONDS);

        // assertTrue("SU:" + suContents, suContents.valid());
        assertTrue("SP:" + spCount, spCount.valid());
        assertTrue("SPContents:" + spContents, spContents.valid());
    }

    @SuppressWarnings("serial")
    static Function<String, Integer> stringToInt() {
        return new Function<String, Integer>() {

            @Override
            public Integer apply(String v1) {
                return Integer.valueOf(v1);
            }
        };
    }

    @SuppressWarnings("serial")
    static Function<String, Integer> stringToIntExcept68() {
        return new Function<String, Integer>() {

            @Override
            public Integer apply(String v1) {
                Integer i = Integer.valueOf(v1);
                return (i == 68) ? null : i;
            }
        };
    }

    @SuppressWarnings("serial")
    static Function<Integer, Integer> add17() {
        return new Function<Integer, Integer>() {

            @Override
            public Integer apply(Integer v1) {
                return v1 + 17;
            }
        };
    }

    @SuppressWarnings("serial")
    static Function<String, Iterable<String>> splitWords() {
        return new Function<String, Iterable<String>>() {

            @Override
            public Iterable<String> apply(String v1) {
                return Arrays.asList(v1.split(" "));
            }
        };
    }
}
