/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2016, 2016                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
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
	/* begin_generated_IBM_copyright_code                               */
	public static final String IBM_COPYRIGHT =
		" Licensed Materials-Property of IBM                              " + //$NON-NLS-1$ 
		" 5724-Y95                                                        " + //$NON-NLS-1$ 
		" (C) Copyright IBM Corp.  2016, 2016    All Rights Reserved.     " + //$NON-NLS-1$ 
		" US Government Users Restricted Rights - Use, duplication or     " + //$NON-NLS-1$ 
		" disclosure restricted by GSA ADP Schedule Contract with         " + //$NON-NLS-1$ 
		" IBM Corp.                                                       " + //$NON-NLS-1$ 
		"                                                                 " ; //$NON-NLS-1$ 
	/* end_generated_IBM_copyright_code                                 */

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
