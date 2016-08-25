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
package com.ibm.streamsx.topology.test.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.ibm.streamsx.topology.context.JobProperties;
import com.ibm.streamsx.topology.jobconfig.JobConfig;

public class JobSubmissionAPITest {
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
    
    @Test
    public void testEmptyJobConfig() {
        JobConfig jc = new JobConfig();
        
        assertNull(jc.getJobGroup());
        assertNull(jc.getJobName());
        
        _testMostlyEmptyJobConfig(jc);
    }
    
    private void _testMostlyEmptyJobConfig(JobConfig jc) {
        assertFalse(jc.hasSubmissionParameters());          
        assertTrue(jc.getSubmissionParameters().isEmpty());    
        assertFalse(jc.hasSubmissionParameters());  // repeat   
    }
    
    @Test
    public void testConstructedJobConfig() {
        JobConfig jc = new JobConfig("groupY", "nameX");
             
        assertEquals("groupY", jc.getJobGroup());
        assertEquals("nameX", jc.getJobName());
        
        _testMostlyEmptyJobConfig(jc);   
    }
    
    @Test
    public void testSettersJobConfig() {
        JobConfig jc = new JobConfig();
        
        jc.setJobGroup("groupYY");
        jc.setJobName("nameXX");
             
        assertEquals("groupYY", jc.getJobGroup());
        assertEquals("nameXX", jc.getJobName());
    }
    
    @Test
    public void testFromConfigProperties() {
        Map<String, Object> config = new HashMap<>();
        
        JobConfig jc = JobConfig.fromProperties(config);
        
        assertNull(jc.getJobGroup());
        assertNull(jc.getJobName());        
        _testMostlyEmptyJobConfig(jc);
        
        config.put(JobProperties.GROUP, "groupB");
        jc = JobConfig.fromProperties(config);
        assertEquals("groupB", jc.getJobGroup()); 
        assertNull(jc.getJobName());
        _testMostlyEmptyJobConfig(jc);
        
        config.clear();
        config.put(JobProperties.NAME, "nameA");
        jc = JobConfig.fromProperties(config);
        assertNull(jc.getJobGroup());
        assertEquals("nameA", jc.getJobName());        
        _testMostlyEmptyJobConfig(jc);

        config.clear();        
        config.put(JobProperties.GROUP, "groupC");
        config.put(JobProperties.NAME, "nameD");
        jc = JobConfig.fromProperties(config);
        assertEquals("groupC", jc.getJobGroup());
        assertEquals("nameD", jc.getJobName());        
        _testMostlyEmptyJobConfig(jc);   
    
    }
    
    @Test
    public void testFromConfigJobConfig() {
        Map<String, Object> config = new HashMap<>();
        
        JobConfig jc = new JobConfig();
        jc.setJobGroup("groupE");
        jc.setJobName("nameF");
        
        config.put(JobProperties.CONFIG, jc);
        JobConfig jcc = JobConfig.fromProperties(config);   
        assertSame(jc, jcc);
        
        config.put(JobProperties.GROUP, "groupC");
        config.put(JobProperties.NAME, "nameD");
        
        jcc = JobConfig.fromProperties(config);   
        assertSame(jc, jcc);
        assertEquals("groupE", jc.getJobGroup());
        assertEquals("nameF", jc.getJobName());        
    }
    @Test(expected=IllegalArgumentException.class)
    public void testFromConfigInvalid() {
        Map<String, Object> config = new HashMap<>();
        config.put(JobProperties.CONFIG, 235);
        JobConfig.fromProperties(config);
    }
}
