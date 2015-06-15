/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertNotNull;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function7.Predicate;
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
}
