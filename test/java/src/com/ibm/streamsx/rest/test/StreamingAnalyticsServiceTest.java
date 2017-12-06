/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import java.io.File;
import java.io.FileReader;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Result;
import com.ibm.streamsx.rest.StreamingAnalyticsService;

/**
 * Basic initial test of StreamingAnalyticsService
 * Test assumes VCAP_SERVICES and STREAMING_ANALYTICS_SERVICE name are setup
 * in order to test.
 */
public class StreamingAnalyticsServiceTest {
    
    @BeforeClass
    public static void checkRunnable() throws Exception {

        String vcapServices = System.getenv("VCAP_SERVICES");
        assumeNotNull(serviceName(), vcapServices);
    }
    
    private static JsonElement services() {
        String vcapServices = System.getenv("VCAP_SERVICES");
        if (vcapServices.startsWith(File.separator))
            return new JsonPrimitive(vcapServices);
        JsonParser parser = new JsonParser();
       
        return parser.parse(vcapServices);
    }  
    
    private static String serviceName() {
        return System.getenv("STREAMING_ANALYTICS_SERVICE_NAME");
    }
    
    public static JsonObject credentials() throws Exception {
        JsonElement _services = services();
        JsonObject services;
        if (_services.isJsonObject()) {
            services = _services.getAsJsonObject();
        } else {
            JsonParser parser = new JsonParser();
            services = parser.parse(new FileReader(_services.getAsString())).getAsJsonObject();
        }
        
        String name = serviceName();
        JsonArray sas = services.get("streaming-analytics").getAsJsonArray();
        for (JsonElement e : sas) {
            JsonObject service = e.getAsJsonObject();
            if (name.equals(service.get("name").getAsString())) {
                return service.getAsJsonObject("credentials");
            }
        }
        
        throw new IllegalStateException("No service!");
    }
    
    @Test
    public void testUsingDefaults()throws Exception {
        use_it(StreamingAnalyticsService.of(null, null));
    }
    
    @Test
    public void testUsingExplicitName()throws Exception {
        use_it(StreamingAnalyticsService.of(null, serviceName()));
    }
    @Test
    public void testUsingExplicitServices()throws Exception {
        use_it(StreamingAnalyticsService.of(services(), null));
    }
    @Test
    public void testUsingExplicit()throws Exception {
        use_it(StreamingAnalyticsService.of(services(), serviceName()));
    }
    
    @Test
    public void testUsingCredentials()throws Exception {
        use_it(StreamingAnalyticsService.of(credentials()));
    }
    @Test
    public void testUsingDescriptor()throws Exception {
        JsonObject desc = new JsonObject();
        desc.addProperty("type", "streaming-analytics");
        desc.addProperty("name", "my-cool-service");
        desc.add("credentials", credentials());
        
        use_it(StreamingAnalyticsService.of(desc));
    }
    
    private void use_it(StreamingAnalyticsService service) throws Exception {
        
        
        assertNotNull(service);
                
        Result<StreamingAnalyticsService, JsonObject> cs = service.checkStatus(false);
        assertNotNull(cs);
        assertSame(service, cs.getElement());
        assertNull(cs.getId());
        assertTrue(cs.isOk());
        assertNotNull(cs.getRawResult());
        
        cs = service.checkStatus(true);
        assertNotNull(cs);
        assertSame(service, cs.getElement());
        assertNull(cs.getId());
        assertTrue(cs.isOk());
        assertNotNull(cs.getRawResult());


        
        Instance instance = service.getInstance();
        assertNotNull(instance);      
        assertNotNull(instance.getJobs());
        
        
    }
}
