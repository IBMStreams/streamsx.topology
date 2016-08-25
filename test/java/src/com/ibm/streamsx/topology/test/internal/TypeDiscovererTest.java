/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.internal;


import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.core.TypeDiscoverer;
import com.ibm.streamsx.topology.json.JSONStreams.SerializeJSON;
import com.ibm.streamsx.topology.test.TestTopology;

@SuppressWarnings("serial")
public class TypeDiscovererTest extends TestTopology {

    @Before
    public void checkIsMain() {
        assumeTrue(isMainRun());
    }
   
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
    
    public static class TwoType<R,T> implements Function<T,T>, Callable<R> {
        @Override
        public T apply(T v) {
            return v;
        }
        public R call() { return null;}
    }
      
    @Test
    public void testGenericFunctionClass() {
        TypeDiscoverer.determineStreamType(new Identity<java.sql.Blob>(), null);
    }
    
    @Test
    public void testanonymousGenericFunctionClass() {
        TypeDiscoverer.determineStreamType(new Identity<java.sql.Blob>() {} , null);
    }
    
    @Test
    public void testTwoTypeFunction() {
        TypeDiscoverer.determineStreamType(new TwoType<String, java.sql.Blob>() {} , null);
    }
    
    @Test
    public void testJSONSerializer() {
        Type clazz = TypeDiscoverer.determineStreamType(new SerializeJSON(), null);
        
        assertEquals(String.class, clazz);
    }
 
    
    @Test
    public void testList() {
               
        RunnableList alr =  new RunnableList();
        _testList(alr, Runnable.class);
    }
    
    public static class RunnableList extends ArrayList<Runnable> implements List<Runnable> {
        
    }
    
    public static <R> void _testList(List<R> list, Class<R> listClass) {
        Type clazz = TypeDiscoverer.determineStreamTypeFromFunctionArg(List.class, 0, list);
        
        assertEquals(listClass, clazz);        
    }
}