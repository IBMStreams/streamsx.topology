/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.json.java.JSONObject;
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

        preBundle();
        File bundle = bundler.submit(app, config).get();
        
        return submitBundle(bundle, config);
    }
    
    private Future<BigInteger> submitBundle(File bundle, Map<String, Object> config) throws InterruptedException, Exception {
        preInvoke();
        InvokeSubmit submitjob = new InvokeSubmit(bundle);

        BigInteger jobId = submitjob.invoke(config);
        
        return new CompletedFuture<BigInteger>(jobId);
    }
    
    void preInvoke() {
    }
    
    void preBundle() {
        // fail early if invoke preconditions aren't met
        InvokeSubmit.checkPreconditions();
    }
    
    /**
     * Submit directly from a JSON representation of a topology.
     */
    @Override
    public Future<BigInteger> submit(JSONObject json) throws Exception {

    	File bundle = bundler.submit(json).get();
    	
        Map<String, Object> config = Collections.emptyMap(); 
        return submitBundle(bundle, config);
    }
}
