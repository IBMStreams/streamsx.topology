/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context.streams;

import java.io.File;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.tester.ConditionTesterImpl;
import com.ibm.streamsx.topology.internal.tester.TesterRuntime;

public class DistributedTester extends DistributedStreamsContext {

    @Override
    public Type getType() {
        return Type.DISTRIBUTED_TESTER;
    }

    @Override
    void preInvoke(AppEntity entity, File bundle) throws Exception {
        Topology app = entity.app;
        if (app != null && app.hasTester()) {
            TesterRuntime trt = ((ConditionTesterImpl) app.getTester()).getRuntime();
            trt.start(null);
        }
    }
}
