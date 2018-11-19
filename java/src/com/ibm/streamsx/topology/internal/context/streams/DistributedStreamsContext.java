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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Result;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
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
    
    private StreamsConnection getConfigConnection(AppEntity entity) {
    	
    	Map<String,Object> config = entity.config;
    	if (config != null && config.containsKey(ContextProperties.STREAMS_CONNECTION)) {  	    
    		Object conn = config.get(ContextProperties.STREAMS_CONNECTION);   		
    		if (conn instanceof StreamsConnection)
    			return (StreamsConnection) conn;
    	}
    	return null;
    }
    
    public synchronized Instance instance() throws IOException {
    	if (!useRestApi())
    		throw new IllegalStateException(/*internal error*/);
    	
    	return instance;
    }
    
    public synchronized Instance createInstance(AppEntity entity) throws IOException {
    	if (!useRestApi())
    		throw new IllegalStateException(/*internal error*/);
    	    	
		StreamsConnection conn = getConfigConnection(entity);
		if (conn == null) {
		    conn = StreamsConnection.createInstance(null, null, null);
		    
		    if (deploy(entity.submission).has(ContextProperties.SSL_VERIFY)) {		        
		        Boolean verify = deploy(entity.submission).get(ContextProperties.SSL_VERIFY).getAsBoolean();		        
		        conn.allowInsecureHosts(!verify);
		    }
		}

		String instanceName = System.getenv(Util.STREAMS_INSTANCE_ID);
		if (instanceName == null)
			instance = conn.getInstances().get(0);
		else
			instance = conn.getInstance(instanceName);
		return instance;
	}
    

    @Override
    protected void preSubmit(AppEntity entity) {
            	
    	if (getConfigConnection(entity) != null) {  		
    	    // Allow the config to provide a connection.
    		useRestApi.set(true);
    		return;
    	}
    		
    	try {
            InvokeSubmit.checkPreconditions();
            useRestApi.set(false);
    	} catch (IllegalStateException e) {
    		// See if the REST api is setup.
    		Util.getenv(Util.STREAMS_REST_URL);
    		Util.getenv(Util.STREAMS_PASSWORD);
    		useRestApi.set(true);
    	}
    }

    @Override
    Future<BigInteger> invoke(AppEntity entity, File bundle) throws Exception {
        try {
            if (useRestApi()) {
                Future<BigInteger> submit = EXECUTOR.submit(() -> invokeUsingRest(entity, bundle));
                // Wait for it to complete to ensure the results file.
                submit.get();
                return submit;
            }

            InvokeSubmit submitjob = new InvokeSubmit(bundle);

            BigInteger jobId = submitjob.invoke(deploy(entity.submission), null, null);
            
            final JsonObject submissionResult = GsonUtilities.objectCreate(entity.submission, RemoteContext.SUBMISSION_RESULTS);
            submissionResult.addProperty(SubmissionResultsKeys.JOB_ID, jobId.toString());
            submissionResult.addProperty(SubmissionResultsKeys.INSTANCE_ID, Util.getDefaultInstanceId());
            
            return new CompletedFuture<BigInteger>(jobId);
        } finally {
            if (!keepArtifacts(entity.submission))
                bundle.delete();
        }
    }
    
    protected BigInteger invokeUsingRest(AppEntity entity, File bundle) throws Exception {
    	
    	Instance instance = createInstance(entity);

    	Result<Job, JsonObject> result = instance.submitJob(bundle, deploy(entity.submission));
    	
    	result.getRawResult().addProperty(SubmissionResultsKeys.INSTANCE_ID, instance.getId());
    	final JsonObject submissionResult = GsonUtilities.objectCreate(entity.submission, RemoteContext.SUBMISSION_RESULTS);
    	for (Entry<String, JsonElement> kv : result.getRawResult().entrySet())
    	    submissionResult.add(kv.getKey(), kv.getValue());
    	
    	return new BigInteger(result.getId());
    }
}
