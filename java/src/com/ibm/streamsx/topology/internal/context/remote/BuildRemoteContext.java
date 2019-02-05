/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2019
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.context.remote.BuildConfigKeys.KEEP_ARTIFACTS;
import static com.ibm.streamsx.topology.internal.context.remote.BuildConfigKeys.determineBuildConfig;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.graph.GraphKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * Create a build archive and submit it to a remote Streams instance or
 * Streaming Analytics service (in sub-classes).
 * 
 * @param <C>
 *            Context type used to maintain connection info.
 */
public abstract class BuildRemoteContext<C> extends ZippedToolkitRemoteContext {
    
    @Override
    public Type getType() {
        return Type.BUNDLE;
    }

    @Override
    public Future<File> _submit(JsonObject submission) throws Exception {

        JsonObject deploy = deploy(submission);

        C context = createSubmissionContext(deploy);

        JsonObject graph = object(submission, "graph");
        // Use the SPL compatible form of the name to ensure
        // that any strange characters in the name provided by
        // the user are not rejected by the build service.
        String buildName = GraphKeys.splAppName(graph);

        Future<File> archive = super._submit(submission);
        
        report("Building sab");

        File buildArchive = archive.get();

        // SPL generation submission can modify the job config overlay
        JsonObject jco = DeployKeys.copyJobConfigOverlays(deploy);

        try {

            JsonObject buildConfig = determineBuildConfig(deploy, submission);
            if (keepArtifacts(submission))
                buildConfig.addProperty(KEEP_ARTIFACTS, true);

            JsonObject submitResult = submitBuildArchive(context, buildArchive,
                    deploy, jco, buildName, buildConfig);

            final JsonObject submissionResult = GsonUtilities
                    .objectCreate(submission, RemoteContext.SUBMISSION_RESULTS);

            GsonUtilities.addAll(submissionResult, submitResult);

        } finally {
            if (!keepArtifacts(submission))
                buildArchive.delete();
        }

        return archive;
    }

    /**
     * Create context specific exception for the submitBuildArchive.
     * 
     * Separated from submitBuildArchive to allow connection info to be check
     * early, before any creation of the build archive.
     */
    protected abstract C createSubmissionContext(JsonObject deploy) throws Exception;

    /**
     * Build the archive returning submission results in the key
     * RemoteContext.SUBMISSION_RESULTS in the raw result.
     */
    protected abstract JsonObject submitBuildArchive(C context, File buildArchive,
            JsonObject deploy, JsonObject jco, String buildName,
            JsonObject buildConfig) throws Exception;
}
