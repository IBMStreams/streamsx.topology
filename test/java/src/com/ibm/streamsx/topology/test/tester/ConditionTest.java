/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.test.tester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;

public class ConditionTest extends TestTopology {
    @Test
    public void testExactCountGood() throws Exception {
        final Topology topology = new Topology();
        TStream<String> source = topology.strings("hello", "goodbye", "farewell");

        Condition<Long> tupleCount = topology.getTester().tupleCount(source, 3);

        boolean passed = complete(topology.getTester(), tupleCount, 10, TimeUnit.SECONDS);
        assertTrue(passed);

        assertTrue(tupleCount.valid());
        assertEquals(3L, (long) tupleCount.getResult());
    }
    
    @Test
    public void testExactCountBad1() throws Exception {
        final Topology topology = new Topology();
        TStream<String> source = topology.strings("hello", "goodbye", "farewell", "toomuch");

        Condition<Long> tupleCount = topology.getTester().tupleCount(source, 3);

        boolean passed = complete(topology.getTester(), tupleCount, 10, TimeUnit.SECONDS);
        assertFalse(passed);      
        assertFalse(tupleCount.valid());
    }
    
    @Test
    public void testExactCountBad2() throws Exception {
        final Topology topology = new Topology();
        TStream<String> source = topology.strings("hello", "goodbye");

        Condition<Long> tupleCount = topology.getTester().tupleCount(source, 3);

        boolean passed = complete(topology.getTester(), tupleCount, 10, TimeUnit.SECONDS);
        assertFalse(passed);
        assertFalse(tupleCount.valid());
    }
    
    @Test
    public void testAtLeastGood() throws Exception {
        final Topology topology = new Topology();
        String[] data = new String[32];
        Arrays.fill(data, "A");
        TStream<String> source = topology.strings(data);

        Condition<Long> tupleCount = topology.getTester().atLeastTupleCount(source, 26);

        boolean passed = complete(topology.getTester(), tupleCount, 10, TimeUnit.SECONDS);
        assertTrue(passed);

        assertTrue(tupleCount.toString(), tupleCount.valid());
        assertTrue(tupleCount.toString(), tupleCount.getResult() >= 26);
    }
}
