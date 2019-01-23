/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
 */
package com.ibm.streamsx.topology.internal.context.remote;

import java.io.File;
import java.io.IOException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.build.Artifact;
import com.ibm.streamsx.rest.build.Build;
import com.ibm.streamsx.rest.build.BuildService;

public class BuildServiceContext extends BuildRemoteContext<BuildService> {
    
    @Override
    public Type getType() {
        return Type.BUNDLE;
    }

    @Override
    BuildService createSubmissionContext(JsonObject deploy) throws Exception {
        return BuildService.of(StreamsKeys.getBuildServiceURL(deploy), StreamsKeys.getBearerToken(deploy));
    }

    @Override
    JsonObject submitBuildArchive(BuildService context, File buildArchive,
            JsonObject deploy, JsonObject jco, String buildName,
            JsonObject buildConfig) throws Exception {
        
        context.allowInsecureHosts();

        Build build = context.createBuild(buildName, buildConfig);
        
        try {
            
            JsonObject result = new JsonObject();
            JsonObject buildInfo = new JsonObject();
            result.add("build", buildInfo);
            buildInfo.addProperty("name", build.getName());

            build.uploadArchiveAndBuild(buildArchive);

            JsonArray artifacts = new JsonArray();
            buildInfo.add("artifacts", artifacts);
            
            for (Artifact artifact : build.getArtifacts()) {
                File sab = artifact.download(null);
                JsonObject sabInfo = new JsonObject();
                sabInfo.addProperty("name", artifact.getName());
                sabInfo.addProperty("size",artifact.getSize());
                sabInfo.addProperty("location", sab.getAbsolutePath());
                artifacts.add(sabInfo);
            }
            return result;
        } finally {
            try {
                build.delete();
            } catch (IOException e) {
                TRACE.warning(
                        "Exception deleteing build: " + e.getMessage());
            }
        }

       
    }

}
