/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test;

import static com.ibm.streamsx.topology.context.StreamsContextFactory.getStreamsContext;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import org.junit.Before;
import org.junit.BeforeClass;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.version.Product;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.internal.streams.Util;
import com.ibm.streamsx.topology.spl.SPLStream;
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
           
        if (getTesterType() == Type.STANDALONE_TESTER || getTesterType() == Type.DISTRIBUTED_TESTER) {
                       
            File agentJar = new File(System.getProperty("user.home"), ".ant/lib/jacocoagent.jar");
            if (agentJar.exists()) {
                List<String> vmArgs = new ArrayList<>();
                String now = Long.toHexString(System.currentTimeMillis());
                String destFile = "jacoco_" + getTesterType().name() + now + ".exec";
     
                
                String arg = "-javaagent:"
                        + agentJar.getAbsolutePath()
                        + "=destfile="
                        + destFile;
                vmArgs.add(arg);
                config.put(ContextProperties.VMARGS, vmArgs);
            }
        }
            
        // Look for a different compiler
        String differentCompile = System.getProperty(ContextProperties.COMPILE_INSTALL_DIR);
        if (differentCompile != null) {
            config.put(ContextProperties.COMPILE_INSTALL_DIR, differentCompile);
            Util.STREAMS_LOGGER.setLevel(Level.INFO);
        }
    }
    

    private static final AtomicInteger topoCounter = new AtomicInteger();
    private static final String baseName = UUID.randomUUID().toString().replace('-', '_');
    
    /**
     * Create a new topology with a unique name.
     */
    protected static Topology newTopology() {
        Topology t = new Topology();
        return newTopology(t.getName());
    }
    
    /**
     * Create a new topology with a unique name based upon the passed in name.
     */
    protected static Topology newTopology(String name) {
        return new Topology(name + "_" + topoCounter.getAndIncrement() + "_" + baseName);
    }  
    protected static Topology newTopology(String ns, String name) {
        return new Topology(ns, name + "_" + topoCounter.getAndIncrement() + "_" + baseName);
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
    
    public boolean isDistributedOrService() {
        return isStreamingAnalyticsRun() || getTesterType() == Type.DISTRIBUTED_TESTER; 
    }

    /**
     * The main run of tests will be with EMBEDDED_TESTER This allows tests to
     * be only run once, with the main run.
     */
    public boolean isMainRun() {
        return getTesterType() == Type.EMBEDDED_TESTER;
    }
    public boolean isStreamingAnalyticsRun() {
        return getTesterType() == Type.STREAMING_ANALYTICS_SERVICE_TESTER;
    }
    public static boolean hasStreamsInstall() {
        String si = System.getenv("STREAMS_INSTALL");
        return si != null && !si.isEmpty();
    }
    
    public Map<String,Object> getConfig() {
        return config;
    }
    
    private int startupDelay = 20;

    public void setStartupDelay(int delay) {
        startupDelay = delay;
    }

    public int getStartupDelay() {
        
        int additional = 0;
        String startupDelayS = System.getProperty("topology.test.additionalStartupDelay");
        if (startupDelayS != null) {
            try {
                additional = Integer.valueOf(startupDelayS);
            } catch (NumberFormatException e) {
                ;
            }
        }
        return startupDelay + additional;
    }

    
    /**
     * Adds a startup delay based upon the context.
     * @param stream
     * @return
     */
    public <T,S> TStream<T> addStartupDelay(TStream<T> stream) {
         
        if (getTesterType() == Type.DISTRIBUTED_TESTER) {
            return stream.modify(new InitialDelay<T>(getStartupDelay()*1000L));
        }
        return stream;
    }
    public SPLStream addStartupDelay(SPLStream stream) {
        if (getTesterType() == Type.DISTRIBUTED_TESTER) {
            return stream.modify(new InitialDelay<Tuple>(getStartupDelay()*1000L));
        }
        return stream;
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

    /**
     * Assume optional data types support.
     */
    protected void assumeOptionalTypes() {
        checkMinimumVersion("Optional data types support required", 4, 3);
    }
        
    /**
     * Only run a test at a specific minimum version or higher.
     */
    protected void checkMinimumVersion(String reason, int... vrmf) {
        if (!hasStreamsInstall()) {
            // must be at least 4.2
            if (vrmf.length >= 2) {
                if (vrmf[0] == 4) {
                    assumeTrue(2 >= vrmf[1]);
                    return;
                }
            }
            fail("Invalid version supplied!");
        }
        
        switch (vrmf.length) {
        case 3:
            assumeTrue((Product.getVersion().getVersion() > vrmf[0])
                     || (Product.getVersion().getVersion() == vrmf[0] &&
                             Product.getVersion().getRelease() > vrmf[1])
                     || (Product.getVersion().getVersion() == vrmf[0] &&
                             Product.getVersion().getRelease() == vrmf[1] &&
                             Product.getVersion().getMod() >= vrmf[2]));
            break;
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
        
        if (!hasStreamsInstall()) {
            // must be at least 4.2
            if (vrmf.length >= 2) {
                if (vrmf[0] == 4) {
                    assumeTrue(vrmf[1] >= 2);
                    return;
                }
            }
        }
        
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
        
        if (!hasStreamsInstall()) {
            // must be at least 4.2
            if (vrmf.length == 2) {
                if (vrmf[0] == 4) {
                    assumeTrue(vrmf[1] != 2);
                    return;
                }          
            }
            fail("Invalid version supplied!");
        }
        
        switch (vrmf.length) {
        case 2:
            assumeTrue(Product.getVersion().getVersion() != vrmf[0]
                    && Product.getVersion().getRelease() != vrmf[1]);
        case 1:
            assumeTrue(Product.getVersion().getVersion() != vrmf[0]);
            break;
        default:
            fail("Invalid version supplied!");
        }
    }
    
    public void completeAndValidate(TStream<?> output, int seconds, String...contents) throws Exception {
        completeAndValidate(getConfig(), output, seconds, contents);
    }
    
    public void completeAndValidate(Map<String,Object> config,
            TStream<?> output, int seconds, String...contents) throws Exception {
        
        Tester tester = output.topology().getTester();
        
        if (getTesterType() == Type.DISTRIBUTED_TESTER)
            seconds += getStartupDelay();
        
        Condition<List<String>> expectedContents = tester.completeAndTestStringOutput(
                getTesterContext(),
                config,
                output,
                seconds, TimeUnit.SECONDS,
                contents);

        assertTrue("Expected:" + contents, expectedContents.valid());
    }
    
    /**
     * Return a condition that is true if all conditions are valid.
     * The result is a Boolean that indicates if the condition is valid.
     * @param conditions
     * @return
     */
    public static Condition<Boolean> allConditions(final Condition<?> ...conditions) {
        return Condition.all(conditions);
    }
    
    /**
     * Allows a test to perform a IBM Streams BUNDLE build only.
     * 
     * If a test requires specific configuration & external setup
     * to run then it should use this method to test at least
     * the topology can built into a IBM Streams bundle.
     * 
     * When the system property topology.test.external.run is
     * not set or set to false, then this method will perform
     * a build and return true.
     * 
     * If the property is set to true then any configuration/external
     * setup is assumed to have been setup and this method will
     * return false without performing a build.
     *
     * Thus the test goes on to build & execute the topology
     * and test the expected conditions if this method returns false.
     */
    protected boolean testBuildOnly(Topology topology) throws Exception {
        if (!Boolean.getBoolean("topology.test.external.run")) {
            File bundle = (File) getStreamsContext(Type.BUNDLE).submit(topology, getConfig()).get();
            assertNotNull(bundle);
            assertTrue(bundle.exists());
            assertTrue(bundle.isFile());
            bundle.delete();
            return true;
        }
        return false;
    }
    
    /**
     * Can the test generate ADL.
     */
    protected void adlOk() {
        assumeTrue(SC_OK);
        assumeTrue(getTesterType() == StreamsContext.Type.STANDALONE_TESTER);
    }
}
