/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
 */
package com.ibm.streamsx.topology.internal.context.streamsrest;

import static com.ibm.streamsx.topology.generator.spl.SPLGenerator.getSPLCompatibleName;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.build.Artifact;
import com.ibm.streamsx.rest.build.Build;
import com.ibm.streamsx.rest.build.BuildService;
import com.ibm.streamsx.topology.internal.context.remote.BuildRemoteContext;

public class BuildServiceContext extends BuildRemoteContext<BuildService> {
    
    @Override
    public Type getType() {
        return Type.BUNDLE;
    }

    @Override
    protected BuildService createSubmissionContext(JsonObject deploy) throws Exception {
        return BuildService.of(StreamsKeys.getBuildServiceURL(deploy), StreamsKeys.getBearerToken(deploy));
    }

    @Override
    protected JsonObject submitBuildArchive(BuildService context, File buildArchive,
            JsonObject deploy, JsonObject jco, String buildName,
            JsonObject buildConfig) throws Exception {
        
        context.allowInsecureHosts();
        
        buildName = getSPLCompatibleName(buildName) + "_" + randomHex(16);

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
                sabInfo.addProperty("url", artifact.getURL());
                artifacts.add(sabInfo);
            }
            
            postBuildAction(deploy, jco, result);
            
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

    protected void postBuildAction(JsonObject deploy, JsonObject jco, JsonObject result) throws Exception { 
    }
    
    private static final String HEXES = "0123456789ABCDEF";
    private static final int HEXES_L = HEXES.length();

    private static String randomHex(final int length) {
        char[] name = new char[length];
        for (int i = 0; i < length; i++) {
            name[i] = HEXES.charAt(ThreadLocalRandom.current().nextInt(HEXES_L));
        }
        return new String(name);
    }

}
