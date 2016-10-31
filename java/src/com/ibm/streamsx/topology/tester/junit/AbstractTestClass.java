/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
 */
package com.ibm.streamsx.topology.tester.junit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;

import com.ibm.streams.operator.version.Product;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

/**
 * Test class providing utilities for testing of topologies.
 * 
 * Test classes using JUnit extend this class allowing
 * them to focus on what is being tested. This class
 * handles the invocation of the test including supporting
 * the ability for the same test to be run in different
 * environments.
 *
 */
public class AbstractTestClass {
    
    private StreamsContext.Type testerType = getDefaultTesterType();
    private Map<String,Object> config = new HashMap<>();
    
    /*
    ** Setup methods.
    */

    @Before
    public void setTesterType() {
        String testerTypeString = System.getProperty(TestProperties.TESTER_TYPE);

        if (testerTypeString != null) {
            testerType = StreamsContext.Type.valueOf(testerTypeString);
        }
        
        assertTesterType();
    }
    protected void assertTesterType() {
        switch (getTesterType()) {
        case EMBEDDED_TESTER:
        case DISTRIBUTED_TESTER:
        case STANDALONE_TESTER:
            break;
        default:
            fail("Tester context type is invalid: " + getTesterType());
        }
    }
    
    /*
    ** Default values for a test.
    */
    public StreamsContext.Type getDefaultTesterType() {
        return StreamsContext.Type.EMBEDDED_TESTER;
    }
    
    public int getDefaultTimeout() {
        return 10;
    }
    
    /*
    ** Getters for the test environment.
    */

    

    
    public StreamsContext.Type getTesterType() {
        return testerType;
    }
       
    public Map<String,Object> getConfig() {
        return config;
    }
    
    /**
     * Return the default context for tests. Using this allows the tests to be
     * run against different contexts, assuming the results/asserts are expected
     * to be the same.
     */
    public StreamsContext<?> getTesterContext() {
        return StreamsContextFactory.getStreamsContext(getTesterType());
    }
    
    /*
     * Assumptions
     */
    
    protected void splOK() {
        assumeTrue(getTesterType() != StreamsContext.Type.EMBEDDED_TESTER);
    }
    
    /*
    ** Allow control over when a test is run based upon
    *  IBM Streams version.
    */
    
    /**
     * Only run a test at a specific minimum version or higher.
     */
    protected void checkMinimumVersion(String reason, int... vrmf) {
        switch (vrmf.length) {
        case 2:
            assumeTrue((Product.getVersion().getVersion() > vrmf[0])
                     || (Product.getVersion().getVersion() == vrmf[0] &&
                             Product.getVersion().getRelease() >= vrmf[1]));
            break;
        case 1:
            assumeTrue(Product.getVersion().getVersion() >= vrmf[0]);
            break;
        default:
            fail("Invalid version supplied!");
        }
    }
    
    /**
     * Only run a test at a specific maximum version or lower.
     */
    protected void checkMaximumVersion(String reason, int... vrmf) {
        switch (vrmf.length) {
        case 2:
            assumeTrue((Product.getVersion().getVersion() < vrmf[0])
                     || (Product.getVersion().getVersion() == vrmf[0] &&
                             Product.getVersion().getRelease() <= vrmf[1]));
            break;
        case 1:
            assumeTrue(Product.getVersion().getVersion() <= vrmf[0]);
            break;
        default:
            fail("Invalid version supplied!");
        }
    }
    
    /**
     * Allow a test to be skipped for a specific version.
     */
    protected void skipVersion(String reason, int ...vrmf) {
        
        switch (vrmf.length) {
        case 4:
            assumeTrue(Product.getVersion().getFix() != vrmf[3]);
        case 3:
            assumeTrue(Product.getVersion().getMod() != vrmf[2]);
        case 2:
            assumeTrue(Product.getVersion().getRelease() != vrmf[1]);
        case 1:
            assumeTrue(Product.getVersion().getVersion() != vrmf[0]);
            break;
        default:
            fail("Invalid version supplied!");
        }
    }
    
    private final static AtomicLong tc = new AtomicLong();
    /**
     * 
     */
    public Topology newTopology() {
        return new Topology(getClass().getSimpleName() + "_" + tc.getAndIncrement());
    }
    
    /*
    ** Test execution methods.
    */
    
    /**
     * Test a topology that may run forever.
     * If endCondition is null then:
     *   
     * In a distributed environment the 
     */
    public boolean complete(Tester tester, Condition<?> endCondition, long timeout, TimeUnit unit) throws Exception {
        
        return tester.complete(getTesterContext(), getConfig(), endCondition, timeout, unit);
    }
    
    public boolean verifyComplete(Tester tester, Condition<?> endCondition, long timeout, TimeUnit unit) throws Exception {
        
        boolean finished = complete(tester, endCondition, timeout, unit);
        assertTrue(endCondition.getResult().toString(), endCondition.valid());
        return finished;
    }
    
    public void verifyComplete(Tester tester, Condition<?> endCondition) throws Exception {
        verifyComplete(tester, endCondition, getDefaultTimeout(), TimeUnit.SECONDS);
    }
}

