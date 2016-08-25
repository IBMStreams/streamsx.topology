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

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import org.junit.Before;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.test.TestTopology;

/**
 * Test publish/subscribe. These tests just use publish/subscribe
 * within a single job, but the expected use case is across jobs.
 *
 */
abstract class PublishSubscribePython extends TestTopology {
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

    @Before
    public void checkIsDistributed() {
        assumeTrue(getTesterType() == Type.DISTRIBUTED_TESTER);
    }
    
    Path genPythonToolkit(String module) throws IOException, InterruptedException {
    	
    	Path pubsub = Paths.get(getTestRoot().getAbsolutePath(), "python", "pubsub");
    	System.err.println("Pubsub:" + pubsub);
    	
    	
    	Path pyTk = Files.createTempDirectory("pytk").toAbsolutePath();
    	
    	System.err.println("PKTK:" + pyTk);
    	
    	Path pyPackages = Paths.get(System.getProperty("topology.toolkit.release"),
    			"opt", "python", "packages").toAbsolutePath();
    	    	
		ProcessBuilder pb = new ProcessBuilder("python3", module, pyTk.toAbsolutePath().toString());		
		pb.redirectOutput(Redirect.INHERIT);
		pb.redirectError(Redirect.INHERIT);
		
    	Map<String, String> env = pb.environment();
    	env.put("PYTHONPATH", pyPackages.toString());
		
		pb.directory(pubsub.toFile());
		Process proc = pb.start();
		
		assertEquals(0, proc.waitFor());
    	
    	return pyTk;
    }
    
    public void includePythonApp(Topology topology, String module, String composite) throws Exception {
    	Path pyTk = genPythonToolkit(module);
    	    	    	
    	SPL.addToolkit(topology, pyTk.toFile());
    	
    	SPL.invokeOperator(topology, "MainPy", composite,
    			Collections.emptyList(), Collections.emptyList(),
    			Collections.emptyMap());
    }
}
