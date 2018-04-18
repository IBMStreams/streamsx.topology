/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.test.consistent;

import static com.ibm.streamsx.topology.consistent.ConsistentRegionConfig.periodic;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assume.assumeTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.consistent.ConsistentRegionConfig;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * Test topologies with consistent region.
 * Currently effectively testing that the annotation is applied.
 * 
 * We assume Streams product testing tests consistent region itself.
 */
public class ConsistentRegionTest extends TestTopology {
    
    @Before
    public void checkIsDistributed() {
        assumeTrue(isDistributedOrService());
    }
   
    @Test
    public void testConsistentPeriodic() throws Exception {
        Topology topology = new Topology("testConsistentPeriodic");
        
        final int N = 2000;
        
        StreamSchema schema = Type.Factory.getStreamSchema("tuple<uint64 id>");
        Map<String,Object> params = new HashMap<>();
        params.put("iterations", N);
        params.put("period", 0.01);      
        SPLStream b = SPL.invokeSource(topology, "spl.utility::Beacon", params, schema);
        
        ConsistentRegionConfig config = periodic(2);
        assertSame(b, b.setConsistent(config));
        
        // Create a mini-flow
        b = b.filter(t -> true);
        
        Condition<Long> exact = topology.getTester().tupleCount(b, N);
        Condition<Void> resets = topology.getTester().resetConsistentRegions(null);
        assertNotNull(resets);
        complete(topology.getTester(), exact, 80, TimeUnit.SECONDS);
    }
    
    @Test
    public void testConsistentOperatorDriven() throws Exception {
        Topology topology = new Topology("testConsistentOperatorDriven");
        
        final int N = 2000;
        
        StreamSchema schema = Type.Factory.getStreamSchema("tuple<uint64 id>");
        Map<String,Object> params = new HashMap<>();
        params.put("iterations", N);
        params.put("period", 0.01);
        params.put("triggerCount", SPL.createValue(37, Type.MetaType.UINT32));
        
        SPLStream b = SPL.invokeSource(topology, "spl.utility::Beacon", params, schema);
        
        ConsistentRegionConfig config = ConsistentRegionConfig.operatorDriven();
        assertSame(b, b.setConsistent(config));
        
        // Create a mini-flow
        b = b.filter(t -> true);
        
        Condition<Void> resets = topology.getTester().resetConsistentRegions(5);
        assertNotNull(resets);
        
        Condition<Long> exact = topology.getTester().tupleCount(b, N);
        complete(topology.getTester(), exact, 80, TimeUnit.SECONDS);
    }
    
    /**
     * Expected to raise an exception as triggerCount requires it
     * be in a operator driven consistent region. Somewhat
     * enforces that testConsistentOperatorDriven() is doing the correct thing.
     * @throws Exception
     */
    @Test(expected=Exception.class)
    public void testMissingOperatorDrivenConsistent() throws Exception {
        Topology topology = new Topology("testConsistentOperatorDriven");
        
        StreamSchema schema = Type.Factory.getStreamSchema("tuple<uint64 id>");
        Map<String,Object> params = new HashMap<>();
        params.put("iterations", 300);
        params.put("triggerCount", SPL.createValue(37, Type.MetaType.UINT32));
        
        SPLStream b = SPL.invokeSource(topology, "spl.utility::Beacon", params, schema);
        
        Condition<Long> atLeast = topology.getTester().atLeastTupleCount(b, 300);
        complete(topology.getTester(), atLeast, 40, TimeUnit.SECONDS);
    }
}
