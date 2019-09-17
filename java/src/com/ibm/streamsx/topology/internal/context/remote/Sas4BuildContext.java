/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2019
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.SERVICE_NO_CHECK_PERIOD;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.SERVICE_RUNNING_TIME;
import static com.ibm.streamsx.topology.internal.context.remote.RemoteBuildAndSubmitRemoteContext.SERVICE_ACCESS;

import java.io.File;
import java.util.List;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Result;
import com.ibm.streamsx.rest.StreamingAnalyticsService;

/**
 * Build and job submitter using a build archive and the build service
 * for the Streaming Analytics service on public cloud.
 */
public class Sas4BuildContext extends BuildRemoteContext<StreamingAnalyticsService> {
    
	@Override
    public Type getType() {
        return Type.SAS_BUNDLE;
    }
	
    @Override
    protected StreamingAnalyticsService createSubmissionContext(JsonObject deploy)
            throws Exception {
        return RemoteBuildAndSubmitRemoteContext.streamingAnalyticServiceFromDeploy(deploy);
    }
	
    @Override
    protected JsonObject submitBuildArchive(StreamingAnalyticsService sas,
            File buildArchive, JsonObject deploy, JsonObject jco,
            String buildName, JsonObject buildConfig) throws Exception {

        // See if we can avoid a service check from
        // a time in the JSON deploy section or the last
        // time this thread saw the service was running.
        boolean checkIfRunning = true;
        if (deploy.has(SERVICE_RUNNING_TIME)) {
            long last = deploy.get(SERVICE_RUNNING_TIME).getAsLong();
            checkIfRunning = (System.currentTimeMillis()
                    - last) > SERVICE_NO_CHECK_PERIOD;
        } else if (SERVICE_ACCESS.get().containsKey(sas.getName())) {
            long last = SERVICE_ACCESS.get().get(sas.getName());
            checkIfRunning = (System.currentTimeMillis()
                    - last) > SERVICE_NO_CHECK_PERIOD;
        }

        if (checkIfRunning) {
            report("Checking service");
            RemoteContexts.checkServiceRunning(sas);
        }

        report("Building application");
        Result<List<File>, JsonObject> submitResult = sas.build(buildArchive, buildName, buildConfig);

        SERVICE_ACCESS.get().put(sas.getName(), System.currentTimeMillis());

        return submitResult.getRawResult();
    }
}
