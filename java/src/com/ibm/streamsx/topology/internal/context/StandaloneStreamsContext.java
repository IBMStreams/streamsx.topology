/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.context.remote.DeployKeys;
import com.ibm.streamsx.topology.internal.streams.InvokeStandalone;

public class StandaloneStreamsContext extends BundleUserStreamsContext<Integer> {
    public StandaloneStreamsContext() {
        super(true);
    }

    @Override
    public Type getType() {
        return Type.STANDALONE;
    }

    /**
     * Build the SPL bundle, execute as a standalone application and remove the
     * bundle. Note, when this returns the application likely will still be
     * running.
     */
    @Override
    Future<Integer> _submit(Topology app, Map<String, Object> config)
            throws Exception {

        File bundle = bundler._submit(app, config).get();

        InvokeStandalone invokeStandalone = new InvokeStandalone(bundle);

        preInvoke(app);
        Future<Integer> future = invokeStandalone.invoke(config);

        return future;
    }

    void preInvoke(Topology app) {
    }
    
    @Override
    public Future<Integer> submit(JSONObject json) throws Exception {

    	File bundle = bundler.submit(json).get();
        InvokeStandalone invokeStandalone = new InvokeStandalone(bundle);
        JSONObject deploy = (JSONObject) json.get("deploy");
        if (deploy != null) {
            JSONObject python = (JSONObject) deploy.get(DeployKeys.PYTHON);
            if (python != null)
                invokeStandalone.addEnvironmentVariable("PYTHONHOME", python.get("prefix").toString());
        }

        Map<String, Object> config = Collections.emptyMap();
        Future<Integer> future = invokeStandalone.invoke(config);

        return future;
    }
}
