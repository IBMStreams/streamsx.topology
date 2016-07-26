/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.python;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;

/**
 * Test publish/subscribe. These tests just use publish/subscribe
 * within a single job, but the expected use case is across jobs.
 *
 */
public class PublishSubscribeStringPythonTest extends PublishSubscribePython {

	/**
	 * String Subscribe feeding a map
	 */
    @Test
    public void testPublishStringMap() throws Exception {
    	
        final Topology t = new Topology();
  	
    	includePythonApp(t, "string_map_string.py", "str_map_str::str_map_str");
   	    	
        TStream<String> source = t.strings("wasJava", "457", "CrystalPalace");
        
        source = addStartupDelay(source);
        
        source.publish("pytest/string/map");
        
        TStream<String> subscribe = t.subscribe("pytest/string/map/result", String.class);

        completeAndValidate(subscribe, 30, "wasJava_Python234", "457_Python234", "CrystalPalace_Python234");
    }
    
	/**
	 * String Subscribe feeding a filter
	 */
    @Test
    public void testPublishStringFilter() throws Exception {
    	
        final Topology t = new Topology();
  	
    	includePythonApp(t, "string_filter_string.py", "str_filter_str::str_filter_str");
   	    	
        TStream<String> source = t.strings("ABC", "DEF", "4372", "34", "24234XXX");
        
        source = addStartupDelay(source);
        
        source.publish("pytest/string/filter");
        
        TStream<String> subscribe = t.subscribe("pytest/string/filter/result", String.class);

        completeAndValidate(subscribe, 30, "ABC", "DEF", "34");
    }

    @Test
    public void testPublishStringFlatMap() throws Exception {
    	
        final Topology t = new Topology();
  	
    	includePythonApp(t, "string_flatmap_string.py", "str_flatmap_str::str_flatmap_str");
   	    	
        TStream<String> source = t.strings("mary had a little lamb", "If you can keep your head when all about you");
        
        source = addStartupDelay(source);
        
        source.publish("pytest/string/flatmap");
        
        TStream<String> subscribe = t.subscribe("pytest/string/flatmap/result", String.class);

        completeAndValidate(subscribe, 60,
        		"mary", "had", "a", "little", "lamb", "If", "you", "can", "keep", "your", "head", "when", "all", "about", "you");
    }
}
