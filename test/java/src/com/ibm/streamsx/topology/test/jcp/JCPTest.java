/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.jcp;

import static org.junit.Assume.assumeTrue;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;

public class JCPTest extends TestTopology {
    
    @Before
    public void ensureDistributed() {
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);
        assumeTrue(SC_OK);
    }
    
    @Test
    public void testJCP() throws Exception {
        
        Topology topology = newTopology("testSingleJCP");
        topology.addJobControlPlane();
        
        submitAndCheckJCPIsThere(topology);        
    }
    
    /**
     * TODO - Add some mechanism to check that the JCP is indeed there!
     */
    private void submitAndCheckJCPIsThere(Topology topology) throws Exception {
        
        TStream<String> strings = topology.constants(Collections.singletonList("Hello JCP!"));
        
        Condition<Long> end = topology.getTester().tupleCount(strings, 1);
        complete(topology.getTester(), end, 10, TimeUnit.SECONDS);      
    }
    
    /**
     * Test that calling addJobControlPlane doesn't result
     * in multiple invocations of the SPL JobControlPlane operator.
     * @throws Exception
     */
    @Test
    public void testMultiJCPCalls() throws Exception {
        
        Topology topology = newTopology("testMultiJCPCalls");
        
        for (int i = 0 ; i < 10; i++)
            topology.addJobControlPlane();
              
        submitAndCheckJCPIsThere(topology);        
    }
}
