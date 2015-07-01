/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.streamsx.topology.Topology;
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
    public Future<BigInteger> submit(Topology app, Map<String, Object> config)
            throws Exception {

        // Wait to create the bundle.
        preBundle();
        File bundle = bundler.submit(app, config).get();

        preInvoke();
        return streamtoolSubmit(bundle);
    }

    private Future<BigInteger> streamtoolSubmit(File bundle) throws Exception {
        InvokeSubmit submitjob = new InvokeSubmit(bundle);
        BigInteger jobId = submitjob.invoke();
        return new CompletedFuture<BigInteger>(jobId);
    }
    
    void preInvoke() {
    }
    
    void preBundle() {
        // fail early if invoke preconditions aren't met
        InvokeSubmit.checkPreconditions();
    }
}
