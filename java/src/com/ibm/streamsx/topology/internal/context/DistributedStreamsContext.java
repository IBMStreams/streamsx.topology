/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;

import java.io.File;
import java.math.BigInteger;
import java.util.concurrent.Future;

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
    void preSubmit(AppEntity entity) {
        InvokeSubmit.checkPreconditions();
    }

    @Override
    Future<BigInteger> invoke(AppEntity entity, File bundle) throws Exception {

        InvokeSubmit submitjob = new InvokeSubmit(bundle);

        BigInteger jobId = submitjob.invoke(deploy(entity.submission));
        
        return new CompletedFuture<BigInteger>(jobId);
    }
}
