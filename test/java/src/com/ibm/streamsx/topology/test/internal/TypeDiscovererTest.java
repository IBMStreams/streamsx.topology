/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.internal;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Type;

import org.junit.Test;

import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.core.TypeDiscoverer;
import com.ibm.streamsx.topology.json.JSONStreams.SerializeJSON;

@SuppressWarnings("serial")
public class TypeDiscovererTest {
    
   
    @Test
    public void testAnonymousFunction() {
        Type clazz = TypeDiscoverer.determineStreamType(new Function<Integer, String>() {

            @Override
            public String apply(Integer v) {
                return v.toString();
            }}, null);
        
        assertEquals(String.class, clazz);
    }
    @Test
    public void testAnonymousSupplier() {
        Type clazz = TypeDiscoverer.determineStreamType(new Supplier<Runnable>() {

            @Override
            public Runnable get() {
                return null;
            }}, null);
        
        assertEquals(Runnable.class, clazz);
    }
    
    public static class IntToBoolean implements Function<Integer,Boolean> {
        @Override
        public Boolean apply(Integer v) {
            return null;
        }     
    }
    
    public static class I2B extends IntToBoolean {}
    
    @Test
    public void testFunctionClass() {
        Type clazz = TypeDiscoverer.determineStreamType(new IntToBoolean(), null);
        
        assertEquals(Boolean.class, clazz);
    }
    @Test
    public void testExtendedFunctionClass() {
        Type clazz = TypeDiscoverer.determineStreamType(new I2B(), null);
        
        assertEquals(Boolean.class, clazz);
    }
    
    public static class Identity<T> implements Function<T,T> {
        @Override
        public T apply(T v) {
            return v;
        }
    }
      
    @Test(expected=IllegalArgumentException.class)
    public void testGenericFunctionClass() {
        Type clazz = TypeDiscoverer.determineStreamType(new Identity<java.sql.Blob>(), null);
        
        assertNull(clazz);
    }
    
    @Test
    public void testJSONSerializer() {
        Type clazz = TypeDiscoverer.determineStreamType(new SerializeJSON(), null);
        
        assertEquals(String.class, clazz);
    }
 
    /*
    @Test
    public void testList() {
        ArrayList<Runnable> alr = new ArrayList<>();
        List<Runnable> lr = alr;
        _testList(lr, Runnable.class);
    }
    
    public static <R> void _testList(List<R> list, Class<R> listClass) {
        Class<R> clazz = TypeDiscoverer.determineStreamType(list, null);
        
        assertEquals(listClass, clazz);        
    }
    */
}