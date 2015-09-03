/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.streams.BeaconStreams;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;

public class CheckpointTest extends TestTopology {
    
    @Before
    public void checkIsDistributed() {
        assumeTrue(SC_OK);
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);
    }
    
    @Test
    @Ignore("Waiting to implement the SPL generation")
    public void testPeriodicCheckpoint() throws Exception {
        assumeTrue(SC_OK);
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);

        final Topology topology = new Topology();
        topology.checkpointPeriod(2, TimeUnit.SECONDS);
        
        TStream<Long> lb = BeaconStreams.longBeacon(topology, 200);
        TStream<Long> b = lb.throttle(100, TimeUnit.MILLISECONDS);
        
        lb.colocate(b);        
        b = b.filter(new CrashAfter<Long>(37));
        lb.colocate(b);
        
        TStream<String> sb = StringStreams.toString(b.isolate());
        
        Condition<Long> atLeast = topology.getTester().atLeastTupleCount(sb, 100);
        Condition<List<String>> output = topology.getTester().stringContents(sb);
        
        complete(topology.getTester(), atLeast, 90, TimeUnit.SECONDS);
        
        Long starting = null;
        long last = -1;
        int count = 0;
        for (String ls : output.getResult()) {
            long l = Long.valueOf(ls);
            count++;
            if (starting == null) {
                assertEquals(0L, l);
                starting = last = l;
                continue;
            }
            if (l > last) {
                assertEquals(last+1L, l);
                last = l;
                continue;
            }
            
            // Has crashed and restarted,
            assertEquals(38, count);
            
            // Assert that there was a successful
            // checkpoint that increased the value
            assertTrue(l > starting);
            starting = last = l;
        }
    }
    
    /**
     * Crash (exit the process) after N tuples.
     */
    public static class CrashAfter<T> implements Predicate<T> {

        private static final long serialVersionUID = 1L;
        private final int crashAt;
        private transient int counter;
        
        public CrashAfter(int crashAt) {
            this.crashAt = crashAt;
        }

        @Override
        public boolean test(T tuple) {
            if (counter++ == crashAt)
                System.exit(1);
            return true;
        }
        
    }
}
