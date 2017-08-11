/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context.streams;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
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
    Future<Integer> invoke(AppEntity entity, File bundle) throws Exception {
    	
        InvokeStandalone invokeStandalone = new InvokeStandalone(bundle, keepArtifacts(entity.submission));
        JsonObject deploy = deploy(entity.submission);
        JsonObject python = object(deploy, DeployKeys.PYTHON);
        if (python != null)
            invokeStandalone.addEnvironmentVariable("PYTHONHOME", jstring(python, "prefix"));

        Future<Integer> future = invokeStandalone.invoke(deploy);

        return future;
    }
}
