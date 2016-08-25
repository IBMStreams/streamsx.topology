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
import static org.junit.Assume.assumeTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.internal.spljava.Schemas;
import com.ibm.streamsx.topology.jobconfig.JobConfig;
import com.ibm.streamsx.topology.spl.JavaPrimitive;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.test.api.JobPropertiesTest.JobPropertiesTestOp;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * Test submission using JobConfig API
 */
public class JobConfigSubmissionTest extends TestTopology {
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
    public void testGroupJobConfig() throws Exception {
        
        JobConfig config = new JobConfig();
        config.setJobGroup("default");
        List<String> result = testItDirect("testGroupJobConfig", config);
        
        assertFalse(result.get(0).isEmpty()); // job id       
        assertFalse(result.get(2).isEmpty()); // job name
        assertEquals("default", result.get(2)); // job group
        assertEquals("<empty>", result.get(3)); // data-directory
    }
    @Test
    public void testNameJobConfig() throws Exception {
        
        JobConfig config = new JobConfig();
        config.setJobName("nameG");
        List<String> result = testItDirect("testNameJobConfig", config);
        
        assertFalse(result.get(0).isEmpty()); // job id
        assertEquals("nameG", result.get(1)); // job name
        assertEquals("default", result.get(2)); // job group
        assertEquals("<empty>", result.get(3)); // data-directory
    }
    
    @Test
    public void testDataDirJobConfig() throws Exception {
        
        JobConfig config = new JobConfig();
        config.setJobName("nameDD");
        config.setDataDirectory("/tmp/some/dir");
        List<String> result = testItDirect("testNameJobConfig", config);
        
        assertFalse(result.get(0).isEmpty()); // job id
        assertEquals("nameDD", result.get(1)); // job name
        assertEquals("default", result.get(2)); // job group
        assertEquals("/tmp/some/dir", result.get(3)); // data-directory
    }
    
    private List<String> testItDirect(String topologyName, JobConfig config)
            throws Exception {

        // JobConfig only apply to DISTRIBUTED submit
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);
        assumeTrue(SC_OK);
        
        config.addToConfig(getConfig());

        Topology topology = newTopology(topologyName);
        topology.addClassDependency(JobPropertiesTestOp.class);
        SPLStream sourceSPL = JavaPrimitive.invokeJavaPrimitiveSource(topology, JobPropertiesTestOp.class,
                Schemas.STRING, null);
        TStream<String> source = sourceSPL.toStringStream();

        Condition<Long> end = topology.getTester().tupleCount(source, 4);
        Condition<List<String>> result = topology.getTester().stringContents(source);
        complete(topology.getTester(), end, 10, TimeUnit.SECONDS);
        
        return result.getResult();
    }
    
}
