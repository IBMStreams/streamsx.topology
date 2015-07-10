/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;

public class TopologySourceTest extends TestTopology {

    @Test
    public void testLimitedSource() throws Exception {
        Topology topology = new Topology("testLimitedSource");

        TStream<String> ts = topology.limitedSource(new SupplierSource(), 4,
                String.class);

        Condition<List<String>> c = topology.getTester().stringContents(ts, "A0", "A1", "A2",
                "A3");

        complete(topology.getTester(), c, 10, TimeUnit.SECONDS);

        assertTrue(c.toString(), c.valid());
    }

    private static class SupplierSource implements Supplier<String> {
        private static final long serialVersionUID = 1L;
        private transient int i;

        @Override
        public String get() {
            return "A" + i++;
        }
    }

    @Test
    public void testLimitedSourceN() throws Exception {
        Topology topology = new Topology("testLimitedSourceN");

        TStream<String> ts = topology.limitedSourceN(new FunctionalSource(), 3,
                String.class);

        Condition<List<String>> c = topology.getTester().stringContents(ts, "B3", "B4", "B5");

        complete(topology.getTester(), c, 10, TimeUnit.SECONDS);

        assertTrue(c.toString(), c.valid());
    }

    private static class FunctionalSource implements Function<Long, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String apply(Long i) {
            return "B" + (i + 3);
        }
    }
    
    @Test
    public void testPeriodicSource() throws Exception {
        Topology topology = new Topology();

        TStream<Long> timestamps = topology.periodicSource(new PeriodicSourceTester(), 500, TimeUnit.MILLISECONDS, Long.class);
        TStream<String> st = StringStreams.toString(timestamps);
        
        Condition<Long> c = topology.getTester().atLeastTupleCount(st, 20);
        Condition<List<String>> tuples = topology.getTester().stringContents(st, "notused");

        long start = System.currentTimeMillis();
        complete(topology.getTester(), c, 30, TimeUnit.SECONDS);
        
        
        
        Long lastTime = null;
        for (String t : tuples.getResult()) {
            long time = Long.parseLong(t);
            assertTrue(time >= start);
            
            if (lastTime != null) {
                assertTrue(time >= lastTime);
                long diff = time - lastTime;
                assertTrue("Source get time difference:" + diff, diff > 450);
            }
            
            lastTime = time;
        }
    }
    
    public static class PeriodicSourceTester implements Supplier<Long> {
        private static final long serialVersionUID = 1L;
        @Override
        public Long get() {
            return System.currentTimeMillis();
        }
    }
    
    @Test
    public void testPeriodicMultiSource() throws Exception {
        Topology topology = new Topology();

        TStream<String> ms = topology.periodicMultiSource(new PeriodicMultiSourceTester(),
                500, TimeUnit.MILLISECONDS, String.class);
        
        Condition<Long> ending = topology.getTester().atLeastTupleCount(ms, 60);
        Condition<List<String>> tuples = topology.getTester().stringContents(ms, "notused");

        long start = System.currentTimeMillis();
        complete(topology.getTester(), ending, 30, TimeUnit.SECONDS);
        
        String prevTuple = null;
        Long lastTime = null;
        for (String t : tuples.getResult()) {
            if (prevTuple == null) {
                assertTrue(t.startsWith("A"));
                prevTuple = t;
                continue;
            }
            char c = (char) (prevTuple.charAt(0) + 1);
            if (c == 'D')
                c = 'A';
            assertTrue(c == t.charAt(0));
            prevTuple = t;
            
            long time = Long.parseLong(t.substring(1));
            assertTrue(time >= start);
            
            if (lastTime != null) {
                assertTrue(time >= lastTime);
                if (t.startsWith("A")) {
                    long diff = time - lastTime;
                    assertTrue(Long.toString(diff), diff > 450);
                }
            }
            
            lastTime = time;
            
        }
    }
    
    public static class PeriodicMultiSourceTester implements Supplier<Iterable<String>> {
        private static final long serialVersionUID = 1L;
        @Override
        public Iterable<String> get() {
            long now = System.currentTimeMillis();
            return Arrays.asList("A"+now, "B"+now, "C"+now);
        }
    }
}
