/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.jobconfig.JobConfig;
import com.ibm.streamsx.topology.test.TestTopology;

/**
 * Test submission using JobConfig API
 */
public class JobConfigOverlaysFileTest extends TestTopology {
    
    private File sab;
    private File jcos;
    
    /**
     * Runs these tests when in standalone so they are run
     * for the default top-leve; test target. The JCO
     * is for distributed use only, but here we just
     * create a bundle and never execute it.
     */
    @Before
    public void checkIsStandalone() {
        checkMinimumVersion("JobConfigOverlays", 4, 2);
        assumeTrue(SC_OK);
        assumeTrue(getTesterType() == StreamsContext.Type.STANDALONE_TESTER);
    }
    
    @After
    public void cleanup() {
        if (sab != null)
            sab.delete();
        if (jcos != null)
            jcos.delete();
    }
    
    @SuppressWarnings("unchecked")
    private StreamsContext<File> bundler() {
        return (StreamsContext<File>) StreamsContextFactory.getStreamsContext(Type.BUNDLE);
    }
    /**
     * Assert the returned sab is as expected.
     * @param sab
     * @param namespace
     * @param name
     * @return
     * @throws IOException 
     */
    private JsonObject assertSabGetJcos(Topology topology) throws IOException {
        assertNotNull(sab);
        assertTrue(sab.getName().endsWith(".sab"));
        assertTrue(sab.exists());
        assertTrue(sab.isFile());
        
        String sabName = "com.ibm.streamsx.topology.test." + topology.getName() + ".sab";
        assertEquals(sabName, sab.getName());
        
        String jconame = sab.getName().replace(".sab", "_JobConfig.json");
        jcos = new File(sab.getParentFile(), jconame);
        assertTrue(jcos.exists());
        assertTrue(jcos.isFile());
        
        String jcosstr = new String(Files.readAllBytes(jcos.toPath()), StandardCharsets.UTF_8);
        
        JsonParser parser = new JsonParser();
        
        JsonObject jcos = parser.parse(jcosstr).getAsJsonObject();
        
        assertTrue(jcos.has("jobConfigOverlays"));
        assertTrue(jcos.get("jobConfigOverlays").isJsonArray());
        assertEquals(1, jcos.get("jobConfigOverlays").getAsJsonArray().size());
        
        assertTrue(jcos.get("jobConfigOverlays").getAsJsonArray().get(0).isJsonObject());
        
        return jcos;
    }
    
    private static JsonObject jobConfigOverlay(JsonObject jcos) {
        assertTrue(jcos.has("jobConfigOverlays"));        
        return jcos.get("jobConfigOverlays").getAsJsonArray().get(0).getAsJsonObject();
    }
    
    /**
     * Vanilla deployment when nothing special in the
     * topology exists.
     */
    static void assertDefaultDeployment(JsonObject jcos) {
        JsonObject jco = jobConfigOverlay(jcos);
        assertTrue(jco.has("deploymentConfig"));
        assertTrue(jco.get("deploymentConfig").isJsonObject());
        
        // Now parall channel isolation is not set by default
        JsonObject deployConfig = jco.get("deploymentConfig").getAsJsonObject();
        assertEquals(0, deployConfig.entrySet().size());
        assertMissing(deployConfig, "fusionScheme");
        
        assertMissing(jco, "operatorConfigs");
    }
    
    // "deploymentConfig":{"fusionScheme":"legacy"}}]
    static void assertLegacyDeployment(JsonObject jcos) {
        JsonObject jco = jobConfigOverlay(jcos);
        assertTrue(jco.has("deploymentConfig"));
        assertTrue(jco.get("deploymentConfig").isJsonObject());
        
        JsonObject deployConfig = jco.get("deploymentConfig").getAsJsonObject();
        assertEquals(1, deployConfig.entrySet().size());
        assertTrue(deployConfig.has("fusionScheme"));
        assertTrue(deployConfig.get("fusionScheme").isJsonPrimitive());
        
        assertEquals("legacy", deployConfig.get("fusionScheme").getAsString());
        
        assertMissing(jco, "operatorConfigs");
    }
    
    static void assertMissing(JsonObject obj, String property) {
        assertFalse(obj.has(property));
    }
    static void assertJsonString(JsonObject obj, String property, String expected) {
        assertTrue(obj.has(property));
        assertTrue(obj.get(property).isJsonPrimitive());
        assertEquals(expected, obj.get(property).getAsString());
    }
    static void assertJsonBoolean(JsonObject obj, String property, boolean expected) {
        assertTrue(obj.has(property));
        assertTrue(obj.get(property).isJsonPrimitive());
        assertEquals(expected, obj.get(property).getAsBoolean());
    }
    
    @Test
    public void testNoConfig() throws Exception {
        
        // Just a simple graph, which won't be executed.
        Topology topology = newTopology("testNoConfig");
        topology.constants(Collections.emptyList());
        
        sab = bundler().submit(topology).get();
        JsonObject jcos = assertSabGetJcos(topology);
        assertDefaultDeployment(jcos);
        
        JsonObject jco = jobConfigOverlay(jcos);       
        assertMissing(jco, "jobConfig");
        assertMissing(jco, "operatorConfigs");
        assertMissing(jco, "configInstructions"); 
    }
    
    @Test
    public void testWithIsolate() throws Exception {
        
        // Just a simple graph, which won't be executed.
        Topology topology = newTopology("testNoConfig");
        topology.constants(Collections.emptyList()).isolate().forEach(tuple -> {});
        
        sab = bundler().submit(topology).get();
        JsonObject jcos = assertSabGetJcos(topology);
        assertLegacyDeployment(jcos);
        
        JsonObject jco = jobConfigOverlay(jcos);       
        assertMissing(jco, "jobConfig");
        assertMissing(jco, "operatorConfigs");
        assertMissing(jco, "configInstructions"); 
    }
    
    @Test
    public void testStandalone() throws Exception {
        
        // Just a simple graph, which won't be executed.
        Topology topology = newTopology("testNoConfig");
        topology.constants(Collections.emptyList());
        
        @SuppressWarnings("unchecked")
        StreamsContext<File> sb = (StreamsContext<File>) StreamsContextFactory.getStreamsContext(Type.STANDALONE_BUNDLE);
        
        sab = sb.submit(topology).get();
        String jconame = sab.getName().replace(".sab", "_JobConfig.json");
        jcos = new File(sab.getParentFile(), jconame);
        assertFalse(jcos.exists());
    }
    
    @Test
    public void testJobNameGroup() throws Exception {
        
        // Just a simple graph, which won't be executed.
        Topology topology = newTopology("testJobNameGroup");
        topology.constants(Collections.emptyList());
        
        Map<String, Object> cfg = new HashMap<>();
        {
            JobConfig jc = new JobConfig();
            jc.addToConfig(cfg);

            jc.setJobName("JN432");
            jc.setJobGroup("JG436");
        }

        sab = bundler().submit(topology, cfg).get();

        JsonObject jcos = assertSabGetJcos(topology);
        assertDefaultDeployment(jcos);

        JsonObject jco = jobConfigOverlay(jcos);
        assertMissing(jco, "operatorConfigs");
        assertMissing(jco, "configInstructions");

        assertTrue(jco.has("jobConfig"));
        assertTrue(jco.get("jobConfig").isJsonObject());

        JsonObject jc = jco.get("jobConfig").getAsJsonObject();
        assertJsonString(jc, "jobName", "JN432");
        assertJsonString(jc, "jobGroup", "JG436");
        assertEquals(2, jc.entrySet().size());
    }
    
    @Test
    public void testJobConfigOther() throws Exception {
        
        // Just a simple graph, which won't be executed.
        Topology topology = newTopology("testJobConfigOther");
        topology.constants(Collections.emptyList());
        
        Map<String, Object> cfg = new HashMap<>();
        {
            JobConfig jc = new JobConfig();
            jc.addToConfig(cfg);
            jc.setDataDirectory("/tmp/abc");
            jc.setPreloadApplicationBundles(true);
            jc.setTracing(Level.INFO);
            
            assertEquals("info", jc.getStreamsTracing());
        }

        sab = bundler().submit(topology, cfg).get();

        JsonObject jcos = assertSabGetJcos(topology);
        assertDefaultDeployment(jcos);

        JsonObject jco = jobConfigOverlay(jcos);
        assertMissing(jco, "operatorConfigs");
        assertMissing(jco, "configInstructions");

        assertTrue(jco.has("jobConfig"));
        assertTrue(jco.get("jobConfig").isJsonObject());

        JsonObject jc = jco.get("jobConfig").getAsJsonObject();
        assertMissing(jc, "jobName");
        assertMissing(jc, "jobGroup");
        
        assertJsonString(jc, "dataDirectory", "/tmp/abc");
        assertJsonBoolean(jc, "preloadApplicationBundles", true);
        assertJsonString(jc, "tracing", "info");
        assertEquals(3, jc.entrySet().size());
    }
}
