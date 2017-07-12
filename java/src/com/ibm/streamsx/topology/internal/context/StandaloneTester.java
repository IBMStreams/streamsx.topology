/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.util.concurrent.Future;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.tester.StandaloneTesterContextFuture;
import com.ibm.streamsx.topology.internal.tester.TesterRuntime;
import com.ibm.streamsx.topology.internal.tester.ConditionTesterImpl;

public class StandaloneTester extends StandaloneStreamsContext {

    @Override
    public Type getType() {
        return Type.STANDALONE_TESTER;
    }
    
    @Override
    void preInvoke(AppEntity entity, File bundle) throws Exception {
        Topology app = entity.app;
        if (app != null && app.hasTester()) {
            TesterRuntime trt = ((ConditionTesterImpl) app.getTester()).getRuntime();
            trt.start(null);
        }
    }

    @Override
    Future<Integer> postSubmit(AppEntity entity, Future<Integer> future) {
        Topology app = entity.app;
        if (app == null)
            return future;
        return new StandaloneTesterContextFuture<Integer>(future,
                ((ConditionTesterImpl) app.getTester()).getRuntime());
    }
}
