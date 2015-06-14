/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.streamsx.topology.Topology;
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
    public Future<Integer> submit(Topology app, Map<String, Object> config)
            throws Exception {

        File bundle = bundler.submit(app, config).get();

        InvokeStandalone invokeStandalone = new InvokeStandalone(bundle);

        preInvoke();
        Future<Integer> future = invokeStandalone.invoke(config);

        // bundle.delete();
        return future;
    }

    void preInvoke() {
    }
}
