/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.junit.Before;
import org.junit.BeforeClass;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

/**
 * Root class for topology tests.
 * 
 */
public class TestTopology {

    private static StreamsContext.Type testerType = Type.EMBEDDED_TESTER;

    @BeforeClass
    public static void setTesterType() {
        String testerTypeString = System.getProperty("topology.test.type");

        if (testerTypeString != null) {
            testerType = StreamsContext.Type.valueOf(testerTypeString);
        }
    }
    
    private static File TEST_ROOT;
    
    @BeforeClass
    public static void setTesterRoot() {
        String testerRoot = System.getProperty("topology.test.root");
        
        if (testerRoot != null) {

            TEST_ROOT = new File(testerRoot);

            assertTrue(TEST_ROOT.getPath(), TEST_ROOT.isAbsolute());
            assertTrue(TEST_ROOT.getPath(), TEST_ROOT.exists());
        }
    }
    
    public static File getTestRoot() {
        return TEST_ROOT;
    }
    
    private final Map<String,Object> config = new HashMap<>();
    
    @Before
    public void setupConfig() {
        
        
        List<String> vmArgs = new ArrayList<>();
        config.put(ContextProperties.VMARGS, vmArgs);
        
        if (getTesterType() != Type.EMBEDDED_TESTER) {
           
            File agentJar = new File(System.getProperty("user.home"), ".ant/lib/jacocoagent.jar");
            if (agentJar.exists()) {
                String now = Long.toHexString(System.currentTimeMillis());
                String destFile = "jacoco_" + getTesterType().name() + now + ".exec";
     
                
                String arg = "-javaagent:"
                        + agentJar.getAbsolutePath()
                        + "=destfile="
                        + destFile;
                vmArgs.add(arg);
            }
        }
        // Look for a different compiler
        String differentCompile = System.getProperty(ContextProperties.COMPILE_INSTALL_DIR);
        if (differentCompile != null) {
            config.put(ContextProperties.COMPILE_INSTALL_DIR, differentCompile);
            Topology.STREAMS_LOGGER.setLevel(Level.INFO);
        }
    }
    
    

    /**
     * Get the default tester type.
     * 
     * @return
     */
    public StreamsContext.Type getTesterType() {
        return testerType;
    }

    public boolean isEmbedded() {
        return getTesterType() == Type.EMBEDDED_TESTER;
    }

    /**
     * The main run of tests will be with EMBEDDED_TESTER This allows tests to
     * be only run once, with the main run.
     */
    public boolean isMainRun() {
        return getTesterType() == Type.EMBEDDED_TESTER;
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

    public void complete(Tester tester) throws Exception {
        tester.complete(getTesterContext());
    }
    
    /**
     * Test a topology that may run forever.
     * If endCondition is null then:
     *   
     * In a distributed environment the 
     */
    public boolean complete(Tester tester, Condition<?> endCondition, long timeout, TimeUnit unit) throws Exception {
        
        return tester.complete(getTesterContext(), getConfig(), endCondition, timeout, unit);
    }

    /**
     * Once Junit has been upgraded, by default any sub-class with tests will
     * only be run by the EMBEDDED_TESTER by default. A sub-class overrides this
     * to run in multiple modes (to be tested to see if this idea will actually
     * work!).
     */
    @Before
    public void runOnce() {
        // assumeTrue(isMainRun());
    }

    /**
     * Assume check field for compiling bundles with sc
     */
    public static final boolean SC_OK = Boolean
            .getBoolean("topology.test.sc_ok");

    /**
     * Assume check field for performance tests.
     */
    public static final boolean PERF_OK = Boolean
            .getBoolean("topology.test.perf_ok");
    
    protected void assumeSPLOk() {       
        assumeTrue(getTesterType() != StreamsContext.Type.EMBEDDED_TESTER);
        assumeTrue(SC_OK);
    }
    
    public void completeAndValidate(TStream<?> output, int seconds, String...contents) throws Exception {
        completeAndValidate(getConfig(), output, seconds, contents);
    }
    
    public void completeAndValidate(Map<String,Object> config,
            TStream<?> output, int seconds, String...contents) throws Exception {
        
        Tester tester = output.topology().getTester();
        
        Condition<List<String>> expectedContents = tester.completeAndTestStringOutput(
                getTesterContext(),
                config,
                output,
                seconds, TimeUnit.SECONDS,
                contents);

        assertTrue(expectedContents.toString(), expectedContents.valid());
    }
}