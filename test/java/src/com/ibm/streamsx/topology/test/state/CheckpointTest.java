/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015, 2018 
 */
package com.ibm.streamsx.topology.test.state;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ibm.streams.operator.logging.LogLevel;
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
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER
                || getTesterType() == StreamsContext.Type.STREAMING_ANALYTICS_SERVICE_TESTER);
    }
    
    @Test
    public void testInitializable() throws Exception {
        final Topology topology = new Topology();
        TStream<Long> s = StatefulApp.createApp(topology, true, true);
        
        Condition<Long> atLeast = topology.getTester().atLeastTupleCount(s, 50*50);
        
        complete(topology.getTester(), atLeast, 120, TimeUnit.SECONDS);
        
        assertTrue(atLeast.valid());
    }
    
    @Test
    public void testPeriodicCheckpoint() throws Exception {
        testPeriodicCheckpoint(2, 45);
    }
    
    @Test
    @Ignore("Stream issue with crash before 1st checkpoint")
    public void testPeriodicCheckpointEarlyFail() throws Exception {
        testPeriodicCheckpoint(2000, 45);
    }
    
    private void testPeriodicCheckpoint(int period, final int crashAfterCount) throws Exception {

        final Topology topology = new Topology();
        topology.checkpointPeriod(period, TimeUnit.SECONDS);
        
        TStream<Long> lb = BeaconStreams.longBeacon(topology, 500);
        TStream<Long> b = lb.throttle(100, TimeUnit.MILLISECONDS);
        
        lb.colocate(b);  
        b = b.filter(new CrashAfter<Long>(crashAfterCount));
        lb.colocate(b);
        
        TStream<String> sb = StringStreams.toString(b.isolate());
                
        Condition<Long> atLeast = topology.getTester().atLeastTupleCount(sb, 230);
        Condition<String> outputChecker = topology.getTester().stringTupleTester(sb, new CheckOutput(crashAfterCount));
        
        complete(topology.getTester(), atLeast, 90, TimeUnit.SECONDS);
        
        assertTrue(atLeast.valid());
        assertTrue(outputChecker.valid());       
    }
    
    public static class CheckOutput implements Predicate<String> {
		private static final long serialVersionUID = 1L;

		CheckOutput(int crashAfterCount) {
    		this.crashAfterCount = crashAfterCount;
    	}
           
    	private final int crashAfterCount;
        private Long starting = null;
        private long last = -1;
        private int count = 0;
        
        @Override
        public boolean test(String ls) {

            long l = Long.valueOf(ls);
            count++;
            if (starting == null) {
            	if (l != 0L)
            		return false;
                starting = last = l;
                return true;
            }
               
            if (l > last) {
                if (l == last+1) {  
                	if (count >= crashAfterCount+1)
                		return false;
                    // assertTrue("no crash @" + l, count < crashAfterCount+1);
                    last = l;
                    return true;
                }
            }
            
            // Has crashed and restarted,
            // assertEquals("crash count @" + l, crashAfterCount+1, count);
            if (crashAfterCount+1 != count)
            	return false;
                        
            // Assert that there was a successful
            // checkpoint that increased the value
            // assertTrue("after ckpt @" + l, l > starting);
            if (l <= starting)
            	return false;
            starting = last = l;
            count = 1;
            return true;
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
            System.err.println("CrashAt:" + counter + " -- " + new Date());
            if (counter++ == crashAt) {
                Logger.getAnonymousLogger().log(LogLevel.INFO, "Intentional crash!");
                System.err.println("Intentional crash!");
                System.exit(1);
            }
            return true;
        }
        
    }
}
