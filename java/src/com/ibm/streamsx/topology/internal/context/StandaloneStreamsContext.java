/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.context.remote.DeployKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
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
    Future<Integer> _submit(JsonObject submission) throws Exception {

    	File bundle = bundler._submit(submission).get();
        InvokeStandalone invokeStandalone = new InvokeStandalone(bundle);
        JsonObject python = object(submission, "deploy", DeployKeys.PYTHON);
        if (python != null)
            invokeStandalone.addEnvironmentVariable("PYTHONHOME", jstring(python, "prefix"));

        Map<String, Object> config = Collections.emptyMap();
        Future<Integer> future = invokeStandalone.invoke(config);

        return future;
    }
}
