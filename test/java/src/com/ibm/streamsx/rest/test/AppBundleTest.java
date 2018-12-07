/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
 */

package com.ibm.streamsx.rest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.ApplicationBundle;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Result;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.jobconfig.JobConfig;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLSchemas;

/**
 * Test use of ApplicationBundle including Job submission.
 *
 */
public class AppBundleTest {
	
	private File bundle = null;
	
	private File createBundle(String namespace, String name) throws InterruptedException, ExecutionException, Exception {
		
		
		Topology topo = new Topology(namespace, name);
		
		SPL.invokeSource(topo, "spl.utility::Beacon",
				Collections.singletonMap("period", 1.0), SPLSchemas.STRING);
		
		Object b = StreamsContextFactory.getStreamsContext(Type.BUNDLE).submit(topo).get();
		
		bundle = (File) b;
		return bundle;
	}
	
	@After
	public void tearDown() {
		if (bundle != null)
			bundle.delete();
	}
	
	protected Instance getInstance() throws IOException {
		StreamsConnection sc = StreamsConnection.createInstance(null, null, null);
		sc.allowInsecureHosts(true);

		List<Instance> instances = sc.getInstances();
		if (instances.size() == 1)
			return instances.get(0);
		
		return sc.getInstance(System.getenv("STREAMS_INSTANCE_ID"));	
	}
	
	@Test
	public void testAppBundles() throws Exception {
		String namespace = "App.NS_"  + (int) (Math.random() * 10000.0);
		String name = "AppBundleTest" + System.currentTimeMillis();
		final String appName = namespace + "::" + name;
		
		File bundle = createBundle(namespace, name);
		
		Instance instance = getInstance();
		
		Result<Job, JsonObject> result = instance.submitJob(bundle, null);		
		verifyJob(result, appName, null);
		
		
		String jobName = "ABJN_"+ (int) (Math.random() * 10000.0) + "_" + System.currentTimeMillis();
		JobConfig jc = new JobConfig();
		jc.setJobName(jobName);
		
		JsonObject overlay = new JsonObject();				
		overlay.add("jobConfig", new Gson().toJsonTree(jc));
		
		JsonArray a = new JsonArray();
		a.add(overlay);
		JsonObject jco = new JsonObject();
		jco.add("jobConfigOverlays", a);		
		
		result = instance.submitJob(bundle, jco);		
		verifyJob(result, appName, jobName);

		
		final ApplicationBundle appBundle = instance.uploadBundle(bundle);
		assertNotNull(appBundle);
		verifyJob(appBundle.submitJob(null), appName, null);
		
		jobName = "ABJN2_"+ (int) (Math.random() * 10000.0) + "_" + System.currentTimeMillis();
		jc.setJobName(jobName);
	    overlay.add("jobConfig", new Gson().toJsonTree(jc));
		verifyJob(appBundle.submitJob(jco), appName, jobName);
	}
	
	private void verifyJob(Result<Job, JsonObject> result, String appName, String jobName) throws Exception {
		assertTrue(result.isOk());
		assertNotNull(result.getId());
				
		Job job = result.getElement();
		assertEquals(result.getId(), job.getId());
		
		// Verify we can call it.
		job.refresh();
				
		assertEquals(appName, job.getApplicationName());
		if (jobName != null)
			assertEquals(jobName, job.getName());
		job.cancel();		
	}
}
