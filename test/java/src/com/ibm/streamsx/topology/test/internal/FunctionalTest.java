/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.internal;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.Serializable;
import java.net.Proxy;
import java.util.List;

import org.junit.Test;

import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.logic.ObjectUtils;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;

@SuppressWarnings("serial")
public class FunctionalTest extends TestTopology {
    
   
    @Test
    public void testStatefulLogicTest() {
        assumeTrue(isMainRun());
        
        assertTrue(ObjectUtils.isImmutable(new AllowAll<String>()));
        assertTrue(ObjectUtils.isImmutable(new FinalPrimitive(42)));
        assertTrue(ObjectUtils.isImmutable(new FinalEnum(Thread.State.RUNNABLE)));
        assertTrue(ObjectUtils.isImmutable(new FinalString("42")));
        assertTrue(ObjectUtils.isImmutable(new FinalMixed()));
        
        assertFalse(ObjectUtils.isImmutable(new Primitive()));
        assertFalse(ObjectUtils.isImmutable(new Collection()));
    }

    static class FinalPrimitive implements Supplier<Integer> {
        private final int v;
        FinalPrimitive(int v) {
            this.v = v;
        }
        @Override
        public Integer get() {
            return v;
        }
    }
    
    static class FinalString implements Supplier<String> {
        private final String v;
        FinalString(String v) {
            this.v = v;
        }
        @Override
        public String get() {
            return v;
        }
    }
    
    static class FinalEnum implements Supplier<Thread.State> {
        private final Thread.State v;
        FinalEnum(Thread.State v) {
            this.v = v;
        }
        @Override
        public Thread.State get() {
            return v;
        }
    }
    
    static class Primitive implements Supplier<Integer> {
        private int v;
        Primitive() {
        }
        @Override
        public Integer get() {
            return v++;
        }
    }
    
    static class Collection implements Supplier<List<String>> {
        private final List<String> v = null;
        Collection() {
        }
        @Override
        public List<String> get() {
            return v;
        }
    }
    
    static class FinalMixed implements Serializable {
        @SuppressWarnings("unused")
        private final double d = 0.0;
        @SuppressWarnings("unused")
        private final String s = "";
        @SuppressWarnings("unused")
        private final File f = null;
        @SuppressWarnings("unused")
        private final Proxy.Type pt = null;
        @SuppressWarnings("unused")
        private final Float ff = null;
    }
    
    
    
}