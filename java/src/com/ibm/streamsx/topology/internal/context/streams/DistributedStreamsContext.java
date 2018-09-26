/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context.streams;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;

import java.io.File;
import java.math.BigInteger;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Result;
import com.ibm.streamsx.rest.StreamsConnection;
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

    @Override
    public Type getType() {
        return Type.DISTRIBUTED;
    }
    
    @Override
    protected void preSubmit(AppEntity entity) {
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
    	
    	if (useRestApi.get())
    		return EXECUTOR.submit(() -> invokeUsingRest(entity, bundle));

        try {
            InvokeSubmit submitjob = new InvokeSubmit(bundle);

            BigInteger jobId = submitjob.invoke(deploy(entity.submission));
            
            final JsonObject submissionResult = GsonUtilities.objectCreate(entity.submission, RemoteContext.SUBMISSION_RESULTS);
            submissionResult.addProperty(SubmissionResultsKeys.JOB_ID, jobId.toString());
            
            return new CompletedFuture<BigInteger>(jobId);
        } finally {
            if (!keepArtifacts(entity.submission))
                bundle.delete();
        }
    }
    
    private BigInteger invokeUsingRest(AppEntity entity, File bundle) throws Exception {
		synchronized (this) {
			if (instance == null) {
				StreamsConnection conn = StreamsConnection.createInstance(null, null, null);
				// TODO - allow setting of insecure hosts - for testing now hardcode as false
				conn.allowInsecureHosts(true);
				instance = conn.getInstances().get(0);
			}
		}
    	Result<Job, JsonObject> result = instance.submitJob(bundle, deploy(entity.submission));
    	
    	return new BigInteger(result.getId());
    }
}
