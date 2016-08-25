/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2016, 2016                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.Before;
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
	/* begin_generated_IBM_copyright_code                               */
	public static final String IBM_COPYRIGHT =
		" Licensed Materials-Property of IBM                              " + //$NON-NLS-1$ 
		" 5724-Y95                                                        " + //$NON-NLS-1$ 
		" (C) Copyright IBM Corp.  2016, 2016    All Rights Reserved.     " + //$NON-NLS-1$ 
		" US Government Users Restricted Rights - Use, duplication or     " + //$NON-NLS-1$ 
		" disclosure restricted by GSA ADP Schedule Contract with         " + //$NON-NLS-1$ 
		" IBM Corp.                                                       " + //$NON-NLS-1$ 
		"                                                                 " ; //$NON-NLS-1$ 
	/* end_generated_IBM_copyright_code                                 */
    
    @Before
    public void checkIsDistributed() {
        assumeTrue(SC_OK);
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);
    }
    
    @Test
    public void testPeriodicCheckpoint() throws Exception {
        testPeriodicCheckpoint(2, 45);
    }
    
    private void testPeriodicCheckpoint(int period, final int crashAfterCount) throws Exception {
        assumeTrue(SC_OK);
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);

        final Topology topology = new Topology();
        topology.checkpointPeriod(period, TimeUnit.SECONDS);
        
        TStream<Long> lb = BeaconStreams.longBeacon(topology, 500);
        TStream<Long> b = lb.throttle(100, TimeUnit.MILLISECONDS);
        
        lb.colocate(b);  
        b = b.filter(new CrashAfter<Long>(crashAfterCount));
        lb.colocate(b);
        
        TStream<String> sb = StringStreams.toString(b.isolate());
        
        Condition<Long> atLeast = topology.getTester().atLeastTupleCount(sb, 230);
        Condition<List<String>> output = topology.getTester().stringContents(sb);
        
        complete(topology.getTester(), atLeast, 90, TimeUnit.SECONDS);
        
        System.err.println("RESULT: " + output.getResult());
        
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
                if (l == last+1) {  
                    assertTrue("no crash @" + l, count < crashAfterCount+1);
                    last = l;
                    continue;
                }
            }
            
            // Has crashed and restarted,
            assertEquals("crash count @" + l, crashAfterCount+1, count);
            
            // Assert that there was a successful
            // checkpoint that increased the value
            assertTrue("after ckpt @" + l, l > starting);
            starting = last = l;
            count = 1;
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
