/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tuple.BeaconTuple;

/**
 * Tests to verify that the correct type is determined for a stream.
 *
 */
@SuppressWarnings("serial")
public class AutoTypeTest extends TestTopology {

    @Before
    public void onlyMainRun() {
        assumeTrue(isMainRun());
    }

    @Test
    public void testAutoTransform() {
        _testAutoTransform();
    }
    
    private static void _testAutoTransform() {
        Topology t = new Topology();
        
        TStream<Integer> ints = t.strings("3").transform(new Function<String, Integer>() {

            @Override
            public Integer apply(String v) {
                return null;
            }});
        
        assertEquals(Integer.class, ints.getTupleClass());
    }
    
    @Test
    public void testAutoEndlessSourceClass() {
        _testAutoEndlessSourceClass();
    }
    
    private static void _testAutoEndlessSourceClass() {
        Topology t = new Topology();
        
        TStream<Integer> ints = t.endlessSource(new Supplier<Integer>() {

            @Override
            public Integer get() {
                return 3;
            }});
        
        assertEquals(Integer.class, ints.getTupleClass());
    }
    
    @Test
    public void testAutoSourceClass() {
        _testAutoSourceClass();
    }
    
    private static void _testAutoSourceClass() {
        Topology t = new Topology();
        
        TStream<String> stream = t.source(new Supplier<Iterable<String>>() {

            @Override
            public Iterable<String> get() {
                return Collections.singletonList("testAutoSourceClass");
            }});
        
        assertEquals(String.class, stream.getTupleClass());
        assertEquals(String.class, stream.getTupleType());
    }
    @Test
    public void testAutoSourceList() {
        _testAutoSourceList();
    }
    
    private static void _testAutoSourceList() {
        Topology t = new Topology();
        
        TStream<List<String>> stream = t.source(new Supplier<Iterable<List<String>>>() {

            @Override
            public Iterable<List<String>> get() {
                return Collections.singletonList(Collections.singletonList("testAutoSourceList"));
            }});
        
        assertEquals(null, stream.getTupleClass());
        assertTrue(stream.getTupleType() instanceof ParameterizedType);
        assertEquals(List.class, ((ParameterizedType) stream.getTupleType()).getRawType());
    }
    
    @Test
    public void testAutoEndlessSourceSet() {
        _testAutoEndlessSourceSet();
    }
    
    private static void _testAutoEndlessSourceSet() {
        Topology t = new Topology();
        
        TStream<Set<Integer>> stream = t.endlessSource(new Supplier<Set<Integer>>() {

            @Override
            public Set<Integer> get() {
                return Collections.singleton(3);
            }});
        
        assertNull(stream.getTupleClass());
        assertTrue(stream.getTupleType() instanceof ParameterizedType);
        assertEquals(Set.class, ((ParameterizedType) stream.getTupleType()).getRawType());
    }
    
    @Test
    public void testAutoLimitedSourceClass() {
        _testAutoLimitedSourceClass();
    }
    
    private static void _testAutoLimitedSourceClass() {
        Topology t = new Topology();
        
        TStream<BeaconTuple> stream = t.limitedSource(new Supplier<BeaconTuple>() {

            @Override
            public BeaconTuple get() {
                return new BeaconTuple(0);
            }}, 8);
        
        assertEquals(BeaconTuple.class, stream.getTupleClass());
    }
    
    
    @Test
    public void testListTuples() {
        _testListTuples();
    }
    private static TStream<List<String>> _testListTuples() {
        Topology t = new Topology();
        
        TStream<String> strings = t.strings("a", "b");
        
        TStream<List<String>> words = strings.transform(new Function<String,List<String>>() {

            @Override
            public List<String> apply(String v) {
                return Arrays.asList(v.split(" *"));
            }});
        
        assertNull(words.getTupleClass());
        assertTrue(words.getTupleType() instanceof ParameterizedType);
        ParameterizedType pt = (ParameterizedType) words.getTupleType();
        assertEquals(List.class, pt.getRawType());
        
        return words;
    }
    
    @Test(expected=IllegalStateException.class)
    public void testNoPublishListTuples() {
        TStream<List<String>> words = _testListTuples();
        words.publish("/noway");
    }
    
    @Test
    public void testAutoMultiTransform() {
        _testAutoMultiTransform();
    }
    
    private static void _testAutoMultiTransform() {
        Topology t = new Topology();
        
        TStream<Integer> ints = t.strings("3").multiTransform(new Function<String, Iterable<Integer>>() {

            @Override
            public Iterable<Integer> apply(String v) {
                return null;
            }});
        
        assertEquals(Integer.class, ints.getTupleClass());
    }
    
    
    @Test
    public void testStringConstants() throws Exception {
        Topology t = new Topology();
        TStream<String> strings = t.strings("a", "b", "c");
        assertEquals(String.class, strings.getTupleClass());
        assertEquals(String.class, strings.getTupleType());
    }
    
    @Test
    public void testStringListTyped() throws Exception {
        Topology t = new Topology();
        TStream<String> strings = t.constants(Collections.nCopies(10, "hello")).asType(String.class);
        assertEquals(String.class, strings.getTupleClass());
        assertEquals(String.class, strings.getTupleType());
    }
    
    @Test
    public void testStringListUnTyped() throws Exception {
        Topology t = new Topology();
        TStream<String> strings = t.constants(Collections.nCopies(10, "hello"));
        assertNull( strings.getTupleClass());
        assertNotNull(strings.getTupleType());
    }
    
    @Test
    public void testStringListAutoTypedByFilter() {
        _testStringListAutoTypedByFilter();
    }
    
    private static void _testStringListAutoTypedByFilter() {
    
        Topology t = new Topology();
        TStream<String> strings = t.constants(Collections.nCopies(10, "hello"));
        strings = strings.filter(new Predicate<String>() {

            @Override
            public boolean test(String tuple) {
                return false;
            }});
        assertEquals(String.class, strings.getTupleClass());
        assertEquals(String.class, strings.getTupleType());
    }
   
    // @Test
    public void _testStringsUnion() throws Exception {
        assumeTrue(isEmbedded());
        Topology t = new Topology();
        TStream<String> strings0 = t.strings("a", "b", "c");
        TStream<String> strings1 = t.constants(Collections.nCopies(10, "hello"));
        
        TStream<String> strings = strings0.union(strings1);
        
        strings = StringStreams.toString(strings);
        strings.print();
        
        StreamsContextFactory.getEmbedded().submit(t).get();
    }
}
