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

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streams.InvokeSubmit;

public class DistributedStreamsContext extends
        BundleUserStreamsContext<BigInteger> {

    public DistributedStreamsContext() {
        super(false);
    }

    @Override
    public Type getType() {
        return Type.DISTRIBUTED;
    }
    
    @Override
    protected void preSubmit(AppEntity entity) {
        InvokeSubmit.checkPreconditions();
    }

    @Override
    Future<BigInteger> invoke(AppEntity entity, File bundle) throws Exception {

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
}
