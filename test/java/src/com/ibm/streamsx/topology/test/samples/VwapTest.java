/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.test.samples;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

import vwap.Bargain;
import vwap.Vwap;

public class VwapTest extends TestTopology {

    @Test
    public void testVwap() throws Exception {
        // Invokes an SPL operator so cannot run in embedded.       
        assumeSPLOk();   
        
        TStream<Bargain> bargains = Vwap.createVwapTopology();
        bargains = Vwap.realBargains(bargains);
        
        Topology topology = bargains.topology();
        
        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.atLeastTupleCount(bargains, 2200);

        complete(tester, expectedCount, 120, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
    }

}
