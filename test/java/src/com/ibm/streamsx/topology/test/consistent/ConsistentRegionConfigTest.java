/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.test.consistent;


import static com.ibm.streamsx.topology.consistent.ConsistentRegionConfig.operatorDriven;
import static com.ibm.streamsx.topology.consistent.ConsistentRegionConfig.periodic;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.consistent.ConsistentRegionConfig;
import com.ibm.streamsx.topology.consistent.ConsistentRegionConfig.Trigger;
import com.ibm.streamsx.topology.test.TestTopology;

public class ConsistentRegionConfigTest extends TestTopology {
   
    @Test
    public void testDefaultOpDriven() {
        ConsistentRegionConfig config = operatorDriven();
        checkCRC(config, Trigger.OPERATOR_DRIVEN, -1, 180, 180, 5);
    }
    @Test
    public void testDefaultOpDrivenNew() {
        ConsistentRegionConfig config = new ConsistentRegionConfig();
        checkCRC(config, Trigger.OPERATOR_DRIVEN, -1, 180, 180, 5);
    }
    
    @Test
    public void testDefaultPeriod() {
        ConsistentRegionConfig config = periodic(37);
        checkCRC(config, Trigger.PERIODIC, 37, 180, 180, 5);
    }
    @Test
    public void testDefaultPeriodNew() {
        ConsistentRegionConfig config = new ConsistentRegionConfig(39);
        checkCRC(config, Trigger.PERIODIC, 39, 180, 180, 5);
    }
    
    @Test
    public void testChangeDrain() {
        ConsistentRegionConfig config = operatorDriven().drainTimeout(27);
        checkCRC(config, Trigger.OPERATOR_DRIVEN, -1, 27, 180, 5);
    }
    @Test
    public void testChangeReset() {
        ConsistentRegionConfig config = periodic(4).resetTimeout(99);
        checkCRC(config, Trigger.PERIODIC, 4, 180, 99, 5);
    }
    @Test
    public void testChangeAttempts() {
        ConsistentRegionConfig config = periodic(9).maxConsecutiveResetAttempts(10);
        checkCRC(config, Trigger.PERIODIC, 9, 180, 180, 10);
    }
    
    @Test
    public void testChangeAll() {
        ConsistentRegionConfig config = operatorDriven().maxConsecutiveResetAttempts(11).drainTimeout(32).resetTimeout(200);
        checkCRC(config, Trigger.OPERATOR_DRIVEN, -1, 32, 200, 11);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testInvalidDrain0() {
        operatorDriven().drainTimeout(0);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testInvalidDrainNeg() {
        operatorDriven().drainTimeout(-214);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testInvalidReset0() {
        operatorDriven().resetTimeout(0);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testInvalidResetNeg() {
        operatorDriven().resetTimeout(-2124);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testInvalidAttempts0() {
        operatorDriven().maxConsecutiveResetAttempts(0);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testInvalidAttemptsNeg() {
        operatorDriven().maxConsecutiveResetAttempts(-2);
    }
    
    @Test
    public void testEquals() {
        
        assertFalse(operatorDriven().equals(periodic(2)));
        
        assertEquals(operatorDriven(), new ConsistentRegionConfig());
        assertEquals(periodic(7), new ConsistentRegionConfig(7));
        
        
        assertEquals(operatorDriven(), operatorDriven());
        assertEquals(periodic(9), periodic(9));
        
        assertFalse(operatorDriven().equals(operatorDriven().drainTimeout(15)));
        assertFalse(operatorDriven().equals(operatorDriven().resetTimeout(16)));
        assertFalse(operatorDriven().equals(operatorDriven().maxConsecutiveResetAttempts(17))); 
        
        assertFalse(periodic(9).equals(periodic(11)));
    }
    
    private static void checkCRC(ConsistentRegionConfig config,
            Trigger trigger, int period, long drain, long reset, int attempts) {
        
        assertNotNull(config.getTrigger());
        assertTrue(config.getDrainTimeout() > 0);
        assertTrue(config.getResetTimeout() > 0);
        assertTrue(config.getMaxConsecutiveResetAttempts() > 0);
        
        assertEquals(TimeUnit.SECONDS, config.getTimeUnit());
        
        assertEquals(trigger, config.getTrigger());
        assertEquals(((long) period), config.getPeriod());
        
        assertEquals(drain, config.getDrainTimeout());
        assertEquals(reset, config.getResetTimeout());
        assertEquals(attempts, config.getMaxConsecutiveResetAttempts()); 
        
        assertEquals(config, config);
        
        ConsistentRegionConfig dup;
        if (config.getTrigger() == Trigger.OPERATOR_DRIVEN)
            dup = operatorDriven();
        else
            dup = periodic(period);
        
        dup = dup.drainTimeout(drain).resetTimeout(reset).maxConsecutiveResetAttempts(attempts);
        assertEquals(config, dup);
        assertEquals(config.hashCode(), dup.hashCode());
    }
}
