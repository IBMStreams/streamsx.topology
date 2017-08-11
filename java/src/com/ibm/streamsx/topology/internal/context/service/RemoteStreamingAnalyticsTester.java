/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.topology.internal.context.service;

import java.math.BigInteger;
import java.util.concurrent.Future;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.tester.ConditionTesterImpl;
import com.ibm.streamsx.topology.internal.tester.TesterRuntime;

public class RemoteStreamingAnalyticsTester extends RemoteStreamingAnalyticsServiceStreamsContext {

    @Override
    public Type getType() {
        return Type.STREAMING_ANALYTICS_SERVICE_TESTER;
    }
    
    @Override
    protected Future<BigInteger> postSubmit(com.ibm.streamsx.topology.internal.context.JSONStreamsContext.AppEntity entity,
            Future<BigInteger> future) throws Exception {
                
        Topology app = entity.app;
        if (app != null && app.hasTester()) {
            TesterRuntime trt = ((ConditionTesterImpl) app.getTester()).getRuntime();
            trt.start(entity.submission);
        }
        return future;
    }
}
