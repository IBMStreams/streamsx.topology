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
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.internal.context.remote.BuildRemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

public class BuildServiceContext extends BuildRemoteContext<BuildService> {
    
    @Override
    public Type getType() {
        return Type.BUNDLE;
    }
    
    protected boolean sslVerify(JsonObject deploy) {
        if (deploy.has(ContextProperties.SSL_VERIFY))
            return GsonUtilities.jboolean(deploy, ContextProperties.SSL_VERIFY);                
        return true;
    }

    @Override
    protected BuildService createSubmissionContext(JsonObject deploy) throws Exception {
        return BuildService.of(StreamsKeys.getBuildServiceURL(deploy), StreamsKeys.getBearerToken(deploy));
    }

    @Override
    protected JsonObject submitBuildArchive(BuildService context, File buildArchive,
            JsonObject deploy, JsonObject jco, String buildName,
            JsonObject buildConfig) throws Exception {
            
        if (!sslVerify(deploy))
            context.allowInsecureHosts();
        
        buildName = getSPLCompatibleName(buildName) + "_" + randomHex(16);
        
        report("Building bundle");

        Build build = context.createBuild(buildName, buildConfig);
        
        try {
            
            JsonObject result = new JsonObject();
            result.add(SubmissionResultsKeys.SUBMIT_METRICS, build.getMetrics());
            JsonObject buildInfo = new JsonObject();
            result.add("build", buildInfo);           
            buildInfo.addProperty("name", build.getName());

            build.uploadArchiveAndBuild(buildArchive);

            JsonArray artifacts = new JsonArray();
            buildInfo.add("artifacts", artifacts);
            
            if (!build.getArtifacts().isEmpty()) {
                report("Downloading bundle");
                final long startDownloadSabTime = System.currentTimeMillis();
                for (Artifact artifact : build.getArtifacts()) {
                    File sab = artifact.download(null);
                    JsonObject sabInfo = new JsonObject();
                    sabInfo.addProperty("name", artifact.getName());
                    sabInfo.addProperty("size", artifact.getSize());
                    sabInfo.addProperty("location", sab.getAbsolutePath());
                    sabInfo.addProperty("url", artifact.getURL());
                    artifacts.add(sabInfo);
                }
                final long endDownloadSabTime = System.currentTimeMillis();
                build.getMetrics().addProperty(SubmissionResultsKeys.DOWNLOAD_SABS_TIME,
                        (endDownloadSabTime - startDownloadSabTime));
            }
            
            postBuildAction(deploy, jco, result);
            
            return result;
        } finally {
            try {
                build.delete();
            } catch (IOException e) {
                TRACE.warning(
                        "Exception deleting build: " + e.getMessage());
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
