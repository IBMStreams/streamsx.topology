/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest;

import static com.ibm.streamsx.rest.StreamsRestUtils.TRACE;
import static com.ibm.streamsx.topology.generator.spl.SPLGenerator.getSPLCompatibleName;
import static com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys.CONSOLE_APPLICATION_JOB_URL;
import static com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys.CONSOLE_APPLICATION_URL;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.HttpEntity;
import org.apache.http.auth.AUTH;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.StreamsRestUtils.StreamingAnalyticsServiceVersion;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;
import com.ibm.streamsx.topology.internal.streams.Util;

/**
 * Common code for StreamingAnalyticsService implementation. The actions are
 * similar for both current versions, but authentication is different, and some
 * URLs may be slightly different.
 * <p>
 * Much of this is cut & paste of the code from the original uses in
 * BuildServiceRemoteRESTWrapper and AnalyticsServiceStreamsContext, with
 * version-specific behaviour pushed to abstract methods.
 */
abstract class AbstractStreamingAnalyticsService implements StreamingAnalyticsService {

    final protected JsonObject credentials;
    final protected JsonObject service;
    private final String serviceName;

    // Current value for the authorization header
    protected String authorization;

    // Connection to Streams REST API
    AbstractStreamingAnalyticsConnection streamsConnection;

    AbstractStreamingAnalyticsService(JsonObject service) {
        JsonObject credentials = object(service,  "credentials");
        this.credentials = credentials;
        this.service = service;
        this.serviceName = jstring(service, "name");
    }
    
    @Override
    public final String getName() {
        return serviceName;
    }
    
    synchronized AbstractStreamingAnalyticsConnection streamsConnection() throws IOException {
        if (null == streamsConnection) {
            streamsConnection = createStreamsConnection();
        }
        return streamsConnection;
    }

    JsonObject getServiceStatus(CloseableHttpClient httpClient)
            throws IOException, IllegalStateException {
        String url = getStatusUrl(httpClient);

        HttpGet getStatus = new HttpGet(url);
        getStatus.addHeader(AUTH.WWW_AUTH_RESP, getAuthorization());

        return StreamsRestUtils.getGsonResponse(httpClient, getStatus);
  }

    /** Version-specific authorization header handling. */
    protected abstract String getAuthorization();
    /** Version-specific handling for status URL. */
    protected abstract String getStatusUrl(CloseableHttpClient httpClient)
            throws IOException;
    /** Version-specific handling for job submit URL with file bundle. */
    protected abstract String getJobSubmitUrl(CloseableHttpClient httpClient,
            File bundle) throws IOException, UnsupportedEncodingException;
    /** Version-specific handling for job submit URL with artifact. */
    protected abstract String getJobSubmitUrl(JsonObject build)
            throws IOException, UnsupportedEncodingException;
    /** Version-specific field for job submit response. */
    protected abstract String getJobSubmitId();
    /** Version-specific handling for base builds URL. */
    protected abstract String getBuildsUrl(CloseableHttpClient httpClient)
            throws IOException;
    /** Version-specific to submit a build. */
    protected abstract JsonObject submitBuild(CloseableHttpClient httpclient,
            String authorization, File archive, String buildName) throws IOException;
    /** Version-specific to get build info. */
    protected abstract JsonObject getBuild(String buildId,
            CloseableHttpClient httpclient,
            String authorization) throws IOException;
    /** Version-specific to submit build artifact as job. */
    protected abstract JsonObject submitBuildArtifact(CloseableHttpClient httpclient,
            JsonObject deploy, String authorization, String submitUrl)
            throws IOException;
    /** Version-specific to get build info that includes output. */
    protected abstract JsonObject getBuildOutput(String buildId, String outputId,
            CloseableHttpClient httpclient,
            String authorization) throws IOException;
    /** Version-specific mechanism to get AbstractStreamsConnection. */
    abstract AbstractStreamingAnalyticsConnection createStreamsConnection()
            throws IOException;

    /**
     * Set the current authorization header contents.
     */
    protected void setAuthorization(String authorization) {
        this.authorization = authorization;
    }

    @Override
    public Result<Job, JsonObject> submitJob(File bundle, JsonObject jco) throws IOException {
        final CloseableHttpClient httpClient = StreamsRestUtils.createHttpClient();
        try {

            Util.STREAMS_LOGGER.info("Streaming Analytics service (" + serviceName + "): Submitting bundle : " + bundle.getName() + " to " + serviceName);

            if (null == jco) {
                jco = new JsonObject();
            }

            Util.STREAMS_LOGGER.info("Streaming Analytics service (" + serviceName + "): submit job request:" + jco.toString());

            JsonObject response = postJob(httpClient, service, bundle, jco);
            return addConsoleURLs(jobResult(response));
            
        } finally {
            httpClient.close();
        }
    }
    
    private Result<Job,JsonObject> jobResult(JsonObject response) {
        final String jobId = jstring(response, getJobSubmitId());
        return new ResultImpl<>(jobId != null, jobId, () -> jobId == null ? null : getInstance().getJob(jobId), response);            
    }
    
    @Override
    public Result<StreamingAnalyticsService, JsonObject> checkStatus(boolean requireRunning) throws IOException {
        final CloseableHttpClient httpClient = StreamsRestUtils.createHttpClient();
        try {
            JsonObject response = getServiceStatus(httpClient);

            boolean running =
                    "true".equals(jstring(response, "enabled"))
                    &&
                    "running".equals(jstring(response, "status"));

            if (requireRunning && !running)
                throw new IllegalStateException("Service (" + serviceName + ") is not running!");

            return new ResultImpl<>(running, null, () -> this, response); 
        } finally {
            httpClient.close();
        }

    }

    @Override
    public Result<Job, JsonObject> buildAndSubmitJob(File archive, JsonObject jco,
            String buildName) throws IOException {
        
        JsonObject metrics = new JsonObject();
        metrics.addProperty(SubmissionResultsKeys.SUBMIT_ARCHIVE_SIZE, archive.length());
           	
        CloseableHttpClient httpclient = StreamsRestUtils.createHttpClient();
        try {
            // Set up the build name
            if (null == buildName) {
                buildName = "build";
            }
            buildName = getSPLCompatibleName(buildName) + "_" + randomHex(16);
            buildName = URLEncoder.encode(buildName, StandardCharsets.UTF_8.name());
            // Perform initial post of the archive
            TRACE.info("Streaming Analytics service (" + serviceName + "): submitting build " + buildName);
            final long startUploadTime = System.currentTimeMillis();
            JsonObject buildSubmission = submitBuild(httpclient, getAuthorization(), archive, buildName);
            final long endUploadTime = System.currentTimeMillis();
            metrics.addProperty(SubmissionResultsKeys.SUBMIT_UPLOAD_TIME, (endUploadTime - startUploadTime));
            
            String buildId = jstring(buildSubmission, "id");
            String outputId = jstring(buildSubmission, "output_id");

            // Loop until built
            final long startBuildTime = endUploadTime;
            long lastCheckTime = endUploadTime;
            JsonObject buildStatus = getBuild(buildId, httpclient, getAuthorization());  
            String status = jstring(buildStatus, "status");
            while (!"built".equals(status)) {
                String mkey = SubmissionResultsKeys.buildStateMetricKey(status);
                long now = System.currentTimeMillis();
                long duration;
                if (metrics.has(mkey)) {
                    duration = metrics.get(mkey).getAsLong();                  
                } else {
                    duration = 0;
                }
                duration += (now - lastCheckTime);
                metrics.addProperty(mkey, duration);
                lastCheckTime = now;
                
                // 'building', 'notBuilt', and 'waiting' are all states which can eventualy result in 'built'
                // sleep and continue to monitor
                if (status.equals("building") || status.equals("notBuilt") || status.equals("waiting")) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    buildStatus = getBuild(buildId, httpclient, getAuthorization()); 
                    status = jstring(buildStatus, "status");
                    continue;
                } 
                // The remaining possible states are 'failed', 'timeout', 'canceled', 'canceling', and 'unknown', none of which can lead to a state of 'built', so we throw an error.
                else {
                    TRACE.severe("Streaming Analytics service (" + serviceName + "): The submitted archive " + archive.getName() + " failed to build with status " + status + ".");
                    JsonObject output = getBuildOutput(buildId, outputId, httpclient, getAuthorization());
                    String strOutput = "";
                    if (output != null)
                        strOutput = prettyPrintOutput(output);
                    throw new IllegalStateException("Error submitting archive for compilation: \n" + strOutput);
                }
            }
            final long endBuildTime = System.currentTimeMillis();
            metrics.addProperty(SubmissionResultsKeys.SUBMIT_TOTAL_BUILD_TIME, (endBuildTime - startBuildTime));

            // Now perform archive put
            JsonArray artifacts = array(buildStatus, "artifacts");
            if (artifacts == null || artifacts.size() == 0) {
                throw new IllegalStateException("No artifacts associated with build "
                        + jstring(buildStatus, "id"));
            }
            // TODO: support multiple artifacts associated with a single build.
            JsonObject artifact = artifacts.get(0).getAsJsonObject();
            String submitUrl = getJobSubmitUrl(artifact);

            TRACE.info("Streaming Analytics service (" + serviceName + "): submitting job request.");
            final long startSubmitTime = System.currentTimeMillis();
            JsonObject response = submitBuildArtifact(httpclient, jco,
                    getAuthorization(), submitUrl);
            final long endSubmitTime = System.currentTimeMillis();
            metrics.addProperty(SubmissionResultsKeys.SUBMIT_JOB_TIME, (endSubmitTime - startSubmitTime));
            
            Result<Job,JsonObject> result = jobResult(response);
            result.getRawResult().add(SubmissionResultsKeys.SUBMIT_METRICS, metrics);
            return addConsoleURLs(result);
        } finally {
            httpclient.close();
        }
    }
    
    /**
     * Add any required Streams console URLs into the result.
     */
    private Result<Job,JsonObject> addConsoleURLs(final Result<Job,JsonObject> result) throws IOException {
        
        final JsonObject json = result.getRawResult();
        
        final String appUrl = getInstance().getApplicationConsoleURL();
        json.addProperty(CONSOLE_APPLICATION_URL, appUrl);
        
        String jobUrl = appUrl + "&job=" +
            URLEncoder.encode(json.getAsJsonPrimitive("name").getAsString(), "UTF-8");
        json.addProperty(CONSOLE_APPLICATION_JOB_URL, jobUrl);
               
        return result;
    }

    private String prettyPrintOutput(JsonObject output) {
        StringBuilder sb = new StringBuilder();
        for(JsonElement messageElem : array(output, "output")){
            JsonObject message = messageElem.getAsJsonObject();
            sb.append(message.get("message_text") + "\n");
        }
        return sb.toString();
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

    public Instance getInstance() throws IOException {
        return streamsConnection().getInstance();
    }

    static StreamingAnalyticsService of(JsonObject config) throws IOException {
        
        // Get the VCAP service based on the config, and extract credentials
        JsonObject service = VcapServices.getVCAPService(config);
                
        JsonObject credentials = service.get("credentials").getAsJsonObject();
        StreamingAnalyticsServiceVersion version = StreamsRestUtils.getStreamingAnalyticsServiceVersion(credentials);
        switch (version) {
        case V1:
            return new StreamingAnalyticsServiceV1(service);
        case V2:
            return new StreamingAnalyticsServiceV2(service);
        default:
            throw new IllegalStateException("Unknown Streaming Analytics Service version");
        }
    }
    
    /**
     * Submit an application bundle to execute as a job.
     */
    protected JsonObject postJob(CloseableHttpClient httpClient,
            JsonObject service, File bundle, JsonObject jobConfigOverlay)
            throws IOException {

        String url = getJobSubmitUrl(httpClient, bundle);

        HttpPost postJobWithConfig = new HttpPost(url);
        postJobWithConfig.addHeader(AUTH.WWW_AUTH_RESP, getAuthorization());
        FileBody bundleBody = new FileBody(bundle, ContentType.APPLICATION_OCTET_STREAM);
        StringBody configBody = new StringBody(jobConfigOverlay.toString(), ContentType.APPLICATION_JSON);

        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart("bundle_file", bundleBody)
                .addPart("job_options", configBody).build();

        postJobWithConfig.setEntity(reqEntity);

        JsonObject jsonResponse = StreamsRestUtils.getGsonResponse(httpClient, postJobWithConfig);

        TRACE.info("Streaming Analytics service (" + getName() + "): submit job response:" + jsonResponse.toString());

        return jsonResponse;
    }
}
