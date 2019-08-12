/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2018
 */
package com.ibm.streamsx.topology.internal.context.streams;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Result;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.rest.internal.ICP4DAuthenticator;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.context.streamsrest.DistributedStreamsRestContext;
import com.ibm.streamsx.topology.internal.context.streamsrest.StreamsKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streams.InvokeSubmit;
import com.ibm.streamsx.topology.internal.streams.Util;

public class DistributedStreamsContext extends
        BundleUserStreamsContext<BigInteger> {
	
	private final AtomicBoolean useRestApi = new AtomicBoolean();
	private Instance instance;	

    public DistributedStreamsContext() {
        super(false);
    }
    
    public boolean useRestApi() {
    	return useRestApi.get();
    }

    @Override
    public Type getType() {
        return Type.DISTRIBUTED;
    }
    
    public synchronized Instance instance() throws IOException {
    	if (!useRestApi())
    		throw new IllegalStateException(/*internal error*/);
    	
    	return instance;
    }
    
    private synchronized Instance createInstance(AppEntity entity) throws IOException {
    	if (!useRestApi())
    		throw new IllegalStateException(/*internal error*/);
    	
		Instance instance = RemoteDistributedStreamsContext.getConfigInstance(entity);        
		if (instance == null) {
		    boolean verify = true;
		    if (deploy(entity.submission).has(ContextProperties.SSL_VERIFY))
		        verify = deploy(entity.submission).get(ContextProperties.SSL_VERIFY).getAsBoolean();
		    instance = Instance.ofEndpoint(
		            (String) null, (String) null, (String) null, (String) null,
		            verify);		    
		}
		return instance;
	}
    

    @Override
    protected void preSubmit(AppEntity entity) {
            	
    	if (RemoteDistributedStreamsContext.getConfigInstance(entity) != null) {  		
    	    // Allow the config to provide an instance.
    		useRestApi.set(true);
    		return;
    	}
    		
    	try {
            InvokeSubmit.checkPreconditions();
            useRestApi.set(false);
    	} catch (IllegalStateException e) {
    		// See if the REST api is setup.
    	    Util.getenv(Util.ICP4D_DEPLOYMENT_URL);
    		Util.getenv(Util.STREAMS_INSTANCE_ID);
    		Util.getenv(Util.STREAMS_PASSWORD);
    		useRestApi.set(true);
    	}
    }
    
    @Override
    protected final Future<BigInteger> action(AppEntity entity) throws Exception {
        if (useRemoteBuild(entity, e -> 7))
            return fullRemoteAction(entity);

        return super.action(entity);
    }
    
    /**
     * Called to build and submit using REST.
     * V5 path when remote build occurs due to being forced or
     * non-matching operating system with instance.
     */
    protected Future<BigInteger> fullRemoteAction(AppEntity entity) throws Exception {

        RemoteDistributedStreamsContext.setSubmissionInstance(entity);
        
        DistributedStreamsRestContext rc = new DistributedStreamsRestContext();
        rc.submit(entity.submission);
        JsonObject results = GsonUtilities.objectCreate(entity.submission,
                RemoteContext.SUBMISSION_RESULTS);
        String id = GsonUtilities.jstring(results, "id");
        instance = rc.instance();
        return new CompletedFuture<>(new BigInteger(id));
    }

    /**
     * V4 path with local build and job submission using streamtool.
     */
    @Override
    Future<BigInteger> invoke(AppEntity entity, File bundle) throws Exception {
        try {
            if (useRestApi()) {
                final BigInteger jobId = invokeUsingRest(entity, bundle);
                return new CompletedFuture<>(jobId);
            }

            InvokeSubmit submitjob = new InvokeSubmit(bundle);

            final BigInteger jobId = submitjob.invoke(deploy(entity.submission), null, null);
            
            final JsonObject submissionResult = GsonUtilities.objectCreate(entity.submission, RemoteContext.SUBMISSION_RESULTS);
            submissionResult.addProperty(SubmissionResultsKeys.JOB_ID, jobId.toString());
            submissionResult.addProperty(SubmissionResultsKeys.INSTANCE_ID, Util.getDefaultInstanceId());
            
            return new CompletedFuture<>(jobId);
        } finally {
            if (!keepArtifacts(entity.submission))
                bundle.delete();
        }
    }
    
    /**
     * V5 path when local build occurred,
     * (V5 is always job submission using REST).
     */
    protected BigInteger invokeUsingRest(AppEntity entity, File bundle) throws Exception {
    	
    	instance = createInstance(entity);
    	
    	Result<Job, JsonObject> result = instance.submitJob(bundle, deploy(entity.submission));
    	
    	result.getRawResult().addProperty(SubmissionResultsKeys.INSTANCE_ID, instance.getId());
    	final JsonObject submissionResult = GsonUtilities.objectCreate(entity.submission, RemoteContext.SUBMISSION_RESULTS);
    	for (Entry<String, JsonElement> kv : result.getRawResult().entrySet())
    	    submissionResult.add(kv.getKey(), kv.getValue());

    	return new BigInteger(result.getId());
    }
}
