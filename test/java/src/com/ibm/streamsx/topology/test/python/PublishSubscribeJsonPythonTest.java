/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.python;

import static org.junit.Assume.assumeTrue;

import java.util.Arrays;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;

/**
 * Test publish/subscribe. These tests just use publish/subscribe
 * within a single job, but the expected use case is across jobs.
 *
 */
public class PublishSubscribeJsonPythonTest extends PublishSubscribePython {
	
    @BeforeClass
    public static void checkPython() {
    	String pythonversion = System.getProperty("topology.test.python");
    	assumeTrue(pythonversion == null || !pythonversion.isEmpty());
    }

	/**
	 * Json Subscribe feeding a map
	 */
    @Test
    public void testPublishJsonMap() throws Exception {
    	
    	Random r = new Random();
        final Topology t = new Topology();
        
        JSONObject j1 = new JSONObject();
        j1.put("a", r.nextLong());
        j1.put("b", "Hello:" + r.nextInt(200));

        JSONObject j2 = new JSONObject();
        j2.put("a", r.nextLong());
        j2.put("b", "Goodbye:" + r.nextInt(200));

        JSONObject j3 = new JSONObject();
        j3.put("a", r.nextLong());
        j3.put("b", "So long:" + r.nextInt(200));
        
        String s1 = "R" + j1.get("a") + "X" + j1.get("b") + "X" +
             (((Long) j1.get("a")) + 235L);
        String s2 = "R" + j2.get("a") + "X" + j2.get("b") + "X" +
                (((Long) j2.get("a")) + 235L);
        String s3 = "R" + j3.get("a") + "X" + j3.get("b") + "X" +
                (((Long) j3.get("a")) + 235L);

        
  	
    	includePythonApp(t, "json_map_json.py", "json_map_json::json_map_json");
   	    	
        TStream<JSONObject> source = t.constants(Arrays.asList(j1, j2, j3));
        
        source = addStartupDelay(source).asType(JSONObject.class);
        
        source.publish("pytest/json/map");
        
        TStream<JSONObject> subscribe = t.subscribe("pytest/json/map/result", JSONObject.class);
        
        TStream<String> asString = subscribe.transform(
        		j -> "R" + j.get("a") + "X" + j.get("b") + "X" + j.get("c"));

        completeAndValidate(asString, 30, s1, s2, s3);
    }
    
	/**
	 * Json Subscribe feeding a filter
	 */
    @Test
    public void testPublishJsonFilter() throws Exception {
    	
    	Random r = new Random();
    	
        final Topology t = new Topology();
  	
    	includePythonApp(t, "json_filter_json.py", "json_filter_json::json_filter_json");
    	
        JSONObject j1 = new JSONObject();
        j1.put("a", 23523L);
        j1.put("b", "Hello:" + r.nextInt(200));

        JSONObject j2 = new JSONObject();
        j2.put("a", 7L);
        j2.put("b", "Goodbye:" + r.nextInt(200));

        JSONObject j3 = new JSONObject();
        j3.put("a", 101L);
        j3.put("b", "So long:" + r.nextInt(200));
        
        String s2 = "R" + j2.get("a") + "X" + j2.get("b");
   	    	
        TStream<JSONObject> source = t.constants(Arrays.asList(j1, j2, j3));
        
        source = addStartupDelay(source).asType(JSONObject.class);
        
        source.publish("pytest/json/filter");
        
        TStream<JSONObject> subscribe = t.subscribe("pytest/json/filter/result", JSONObject.class);
        
        TStream<String> asString = subscribe.transform(
        		j -> "R" + j.get("a") + "X" + j.get("b"));

        completeAndValidate(asString, 30, s2);
    }
    
	/**
	 * Json Subscribe feeding a flat map
	 */
    @Test
    public void testPublishJsonFlatMap() throws Exception {
    	
    	Random r = new Random();
        final Topology t = new Topology();
        
        JSONObject j1 = new JSONObject();
        j1.put("a", r.nextLong());
        j1.put("b", "Hello:" + r.nextInt(200));

        JSONObject j2 = new JSONObject();
        j2.put("a", r.nextLong());
        j2.put("b", "Goodbye:" + r.nextInt(200));

        JSONObject j3 = new JSONObject();
        j3.put("a", r.nextLong());
        j3.put("b", "So long:" + r.nextInt(200));
        
        String s1a = j1.get("a").toString();
        String s1b = j1.get("b").toString();

        String s2a = j2.get("a").toString();
        String s2b = j2.get("b").toString();

        String s3a = j3.get("a").toString();
        String s3b = j3.get("b").toString();
  	
    	includePythonApp(t, "json_flatmap_string.py", "json_flatmap_string::json_flatmap_str");
   	    	
        TStream<JSONObject> source = t.constants(Arrays.asList(j1, j2, j3));
        
        source = addStartupDelay(source).asType(JSONObject.class);
        
        source.publish("pytest/json/flatmap");
        
        TStream<String> subscribe = t.subscribe("pytest/json/flatmap/result", String.class);

        completeAndValidate(subscribe, 60, s1a, s1b, s2a, s2b, s3a, s3b);
    }

}
