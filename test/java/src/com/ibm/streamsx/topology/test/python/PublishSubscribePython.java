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

    @Before
    public void checkIsDistributed() {
        assumeTrue(getTesterType() == Type.DISTRIBUTED_TESTER || isStreamingAnalyticsRun());
    }
    
    Path genPythonToolkit(String module) throws IOException, InterruptedException {
    	
    	Path pubsub = Paths.get(getTestRoot().getAbsolutePath(), "python", "pubsub");
    	
    	Path pyTk = Files.createTempDirectory("pytk").toAbsolutePath();
    	
    	Path pyPackages = Paths.get(System.getProperty("topology.toolkit.release"),
    			"opt", "python", "packages").toAbsolutePath();

        String pythonversion = System.getProperty("topology.test.python"); 
    	    	
		ProcessBuilder pb = new ProcessBuilder(pythonversion, module, pyTk.toAbsolutePath().toString());		
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
