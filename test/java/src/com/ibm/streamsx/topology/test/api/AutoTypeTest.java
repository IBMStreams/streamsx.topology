/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;

@SuppressWarnings("serial")
public class AutoTypeTest {
    

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
    public void testAutoSource() {
        _testAutoSource();
    }
    
    private static void _testAutoSource() {
        Topology t = new Topology();
        
        TStream<Integer> ints = t.endlessSource(new Supplier<Integer>() {

            @Override
            public Integer get() {
                return 3;
            }}, null);
        
        assertEquals(Integer.class, ints.getTupleClass());
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
}
