/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.test.TestTopology;

public class JavaTupleTest extends TestTopology {

    @Test
    public void testNumberConstant() throws Exception {
        final Topology topology = new Topology("Simple");
        TStream<Number> source = topology.numbers(8, 32L, 4, new BigInteger("99392"),
                new BigDecimal("45.224452"));
        assertNotNull(source);
        completeAndValidate(source, 10,  "8", "32", "4", "99392", "45.224452");
    }

    @Test
    public void testFilterByObjectType() throws Exception {
        final Topology f = new Topology("SimpleFilter");
        TStream<Number> source = f.numbers(8, 32L, 4, new BigInteger("99392"),
                new BigDecimal("45.224452"));
        TStream<Number> filtered = source.filter(new InstanceFilter());
            
        completeAndValidate(filtered, 10,  "8", "32", "4", "45.224452");
    }

    @SuppressWarnings("serial")
    public static class InstanceFilter implements Predicate<Number> {

        @Override
        public boolean test(Number v1) {
            return !(v1 instanceof BigInteger);
        }

    }
    
    /**
     * Test a class with non-ascii characters in its name can be used.
     */
    @Test
    public void testNonAsciiClass() throws Exception {
    	
    	List<NAŇÃ> nas = new ArrayList<>();
    	nas.add(new NAŇÃ("one"));
    	nas.add(new NAŇÃ("two"));
    	nas.add(new NAŇÃ("three"));
    	
        final Topology topology = new Topology();
        TStream<NAŇÃ> source = topology.constants(nas).asType(NAŇÃ.class);
            
        completeAndValidate(source, 10,  "one-NAŇÃ", "two-NAŇÃ", "three-NAŇÃ");
    }
    
    /**
     * Test two classes with similar non-ascii characters in its name can be used
     * 
     */
    @Test
    public void testNonAsciiClasses() throws Exception {
    	
    	List<NAŇÃ> nas = new ArrayList<>();
    	nas.add(new NAŇÃ("one"));
    	nas.add(new NAŇÃ("two"));
    	nas.add(new NAŇÃ("three"));
    	
    	List<NAÃÃ> naas = new ArrayList<>();
    	naas.add(new NAÃÃ("one"));
    	naas.add(new NAÃÃ("two"));
    	naas.add(new NAÃÃ("three"));
  	
        final Topology topology = new Topology();
        @SuppressWarnings("unused")
        TStream<NAŇÃ> sourceNAŇÃ = topology.constants(nas).asType(NAŇÃ.class);
        TStream<NAÃÃ> sourceNAÃÃ = topology.constants(naas).asType(NAÃÃ.class);
            
        completeAndValidate(sourceNAÃÃ, 10,  "one-NAÃÃ", "two-NAÃÃ", "three-NAÃÃ");
    }
    
    public static class NAŇÃ implements Serializable {
		private static final long serialVersionUID = 1L;
		private final String s;
    	 
    	public NAŇÃ(String s) {
    		this.s = s;
    	}
    	public String toString() {
    		return s + "-NAŇÃ";
    	}
    }
    
    public static class NAÃÃ implements Serializable {
		private static final long serialVersionUID = 1L;
		private final String s;
    	 
    	public NAÃÃ(String s) {
    		this.s = s;
    	}
    	public String toString() {
    		return s + "-NAÃÃ";
    	}
    }
    
    
}
