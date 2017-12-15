/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.cloud;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.StreamingAnalyticsService;
import com.ibm.streamsx.rest.test.StreamingAnalyticsServiceTest;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.AnalyticsServiceProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.test.TestTopology;

public class ServiceSubmissionAPITest extends TestTopology {
    
    private String vcapServices;
    private String serviceName;
    private boolean localInstall;
    
    @Before
    public void checkServiceAndSaveEnv() {
        
        assumeTrue(isStreamingAnalyticsRun());
        
        vcapServices = System.getenv("VCAP_SERVICES");
        serviceName = System.getenv("STREAMING_ANALYTICS_SERVICE_NAME");
        
        String si = System.getenv("STREAMS_INSTALL");
        localInstall = si != null && !si.isEmpty();
        
        assumeTrue(vcapServices != null);
        assumeTrue(serviceName != null);
    }
    
    @Test
    public void testFromEnv() throws Exception {
        submitApp("testFromEnv");
    }
    
    @Test
    public void testFromConfig() throws Exception {
        getConfig().put(AnalyticsServiceProperties.VCAP_SERVICES, vcapServices);
        getConfig().put(AnalyticsServiceProperties.SERVICE_NAME, serviceName);
              
        submitApp("testFromConfig");
    }
    
    @Test
    public void testFromCredentials() throws Exception {
        getConfig().put(AnalyticsServiceProperties.SERVICE_DEFINITION, StreamingAnalyticsServiceTest.credentials());
        
        getConfig().put(AnalyticsServiceProperties.VCAP_SERVICES, "{}");
        getConfig().put(AnalyticsServiceProperties.SERVICE_NAME, "no such service - ignored");
              
        submitApp("testFromCredentials");
    }
    
    @Test
    public void testFromDefinitions() throws Exception {
        
        JsonObject def = new JsonObject();
        def.addProperty("type", "streaming-analytics");
        def.addProperty("name", serviceName);
        def.add("credentials", StreamingAnalyticsServiceTest.credentials());
        
        getConfig().put(AnalyticsServiceProperties.SERVICE_DEFINITION, def);
        
        getConfig().put(AnalyticsServiceProperties.VCAP_SERVICES, "{}");
        getConfig().put(AnalyticsServiceProperties.SERVICE_NAME, "no such service - ignored");
              
        submitApp("testFromDefinition");
    }
    
    
    /**
     * Create a simple app for testing, really only
     * interested if a job can be submitted, not what the job does.
     */
    private TStream<String> createTopology(String name) {
        final Topology topo = newTopology(name);
        
        TStream<String> source = topo.strings("streaming", "analytics", "service");
        
        return source;
    }
    
       
    private void submitApp(String name) throws Exception {
       
        TStream<String> source = createTopology(name);
        
        if (localInstall) {
            submitFromBundle(source.topology());
            return;
        }
 
        // testing service always submits using a remote build.
        completeAndValidate(source, 10, "streaming", "analytics", "service");
    }

    private void submitFromBundle(Topology topology) throws Exception {
        
        @SuppressWarnings("unchecked")
        StreamsContext<BigInteger> ctx = (StreamsContext<BigInteger>) StreamsContextFactory.getStreamsContext("STREAMING_ANALYTICS_SERVICE");
        BigInteger jobId = ctx.submit(topology, getConfig()).get();
        assertNotNull(jobId);
        
        JsonElement vse;
        if (vcapServices.startsWith(File.separator))
            vse = new JsonPrimitive(vcapServices);
        else
            vse = new JsonParser().parse(vcapServices);
        
        StreamingAnalyticsService sas = StreamingAnalyticsService.of(
                vse, serviceName);
        
        Job job = sas.getInstance().getJob(jobId.toString());
        assertNotNull(job);
        boolean canceled = job.cancel();
        assertTrue(canceled);
        
        
    }
}
