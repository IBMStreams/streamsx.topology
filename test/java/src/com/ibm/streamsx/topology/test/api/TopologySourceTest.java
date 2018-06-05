/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.logic.Value;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;

public class TopologySourceTest extends TestTopology {
    
    @Test (expected = NullPointerException.class)
    public void testConstantsNullData() throws Exception {
        assumeTrue(isMainRun());
        Topology t = newTopology("testConstantsNullData");
        t.constants(null);  // throw NPE
    }
    
    @Test (expected = IllegalArgumentException.class)
    public void testLimitedSourceNNegCount() throws Exception {
        assumeTrue(isMainRun());
        Topology t = newTopology("testLimitedSourceNNegCount");
        t.limitedSourceN(new FunctionalSource(), -1);  // throw IAE
    }
    
    @Test (expected = IllegalArgumentException.class)
    public void testLimitedSourceNegCount() throws Exception {
        assumeTrue(isMainRun());
        Topology t = newTopology("testLimitedSourceNegCount");
        t.limitedSource(new Value<String>("s"), -1);  // throw IAE
    }

    @Test
    public void testLimitedSource() throws Exception {
        Topology topology = newTopology("testLimitedSource");

        TStream<String> ts = topology.limitedSource(new SupplierSource(), 4);

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
        Topology topology = newTopology("testLimitedSourceN");

        TStream<String> ts = topology.limitedSourceN(new FunctionalSource(), 3);

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
        // Uses Condition.getResult()
        assumeTrue(!isStreamingAnalyticsRun());
        
        Topology topology = newTopology();

        TStream<Long> timestamps = topology.periodicSource(new PeriodicSourceTester(), 500, TimeUnit.MILLISECONDS);
        TStream<String> st = StringStreams.toString(timestamps);
        
        Condition<Long> c = topology.getTester().atLeastTupleCount(st, 20);
        Condition<List<String>> tuples = topology.getTester().stringContents(st);

        long start = System.currentTimeMillis();
        complete(topology.getTester(), c, 30, TimeUnit.SECONDS);
        
        
        
        Long lastTime = null;
        for (String t : tuples.getResult()) {
            long time = Long.parseLong(t);
            assertTrue(time >= start);
            
            if (lastTime != null) {
                assertTrue(time >= lastTime);
                long diff = time - lastTime;
                assertTrue("Source get time difference:" + diff, diff > 400);
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
        // Uses Condition.getResult()
        assumeTrue(!isStreamingAnalyticsRun());
        
        Topology topology = newTopology();

        TStream<String> ms = topology.periodicMultiSource(new PeriodicMultiSourceTester(),
                500, TimeUnit.MILLISECONDS);
        
        Condition<Long> ending = topology.getTester().atLeastTupleCount(ms, 60);
        Condition<List<String>> tuples = topology.getTester().stringContents(ms);

        long start = System.currentTimeMillis();
        complete(topology.getTester(), ending, 30, TimeUnit.SECONDS);
        
        long totalDiff = 0;
        int count = 0;
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
            assertTrue("Expected time:" + time + ">= start:" + start,
                       time >= start);
            
            if (lastTime != null) {
                assertTrue("Expected time:" + time + ">= lastTime:" + lastTime,
                            time >= lastTime);
                if (t.startsWith("A")) {
                    // Can't really test for a specific diff as the periodic is
                    // at a fixed rate and thread scheduling may mean that the
                    // get was delayed and then called just before the next iteration.
                    
                    long diff = time - lastTime;
                    assertTrue(Long.toString(diff), diff > 0);                    
                    totalDiff += diff;
                    count++;
                }
            }
            lastTime = time;
        }
        
        // Try asserting the average period is somewhat close to 500ms.
        double averageDiff = ((double) totalDiff) / ((double) count);
        assertTrue("Average diff:" + averageDiff, averageDiff > 350.0);
    }
    
    public static class PeriodicMultiSourceTester implements Supplier<Iterable<String>> {
        private static final long serialVersionUID = 1L;
        @Override
        public Iterable<String> get() {
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
            }
            long now = System.currentTimeMillis();
            return Arrays.asList("A"+now, "B"+now, "C"+now);
        }
    }
    
    @Test(expected=ExecutionException.class)
    public void testExceptionSource() throws Exception {
        assumeTrue(isEmbedded());
        
        Topology topology = newTopology();

        TStream<String> ts = topology.limitedSource(new ExceptionSource(), 4);

        Condition<List<String>> c = topology.getTester().stringContents(ts, "A0");

        complete(topology.getTester(), c, 10, TimeUnit.SECONDS);
    }
    
    @Test(expected=ExecutionException.class)
    @Ignore("Test for issue #213")
    public void testExceptionPeriodicSource() throws Exception {
        assumeTrue(isEmbedded());
        
        Topology topology = newTopology();

        TStream<String> ts = topology.periodicSource(new ExceptionSource(), 1, TimeUnit.MILLISECONDS);

        Condition<List<String>> c = topology.getTester().stringContents(ts, "A0");

        complete(topology.getTester(), c, 10, TimeUnit.SECONDS);
    }

    private static class ExceptionSource implements Supplier<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String get() {
            throw new RuntimeException("ExceptionSource testing");
        }
    }
}
