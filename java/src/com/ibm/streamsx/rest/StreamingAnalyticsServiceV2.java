/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest;

import static com.ibm.streamsx.rest.StreamsRestUtils.TRACE;
import static com.ibm.streamsx.topology.generator.spl.SPLGenerator.getSPLCompatibleName;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.client.fluent.Executor;
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
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.rest.internal.RestUtils;
import com.ibm.streamsx.topology.internal.context.remote.BuildConfigKeys;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

class StreamingAnalyticsServiceV2 extends AbstractStreamingAnalyticsService {

    private long authExpiryTime = -1L;

    private String jobSubmitUrl;
    private String buildsUrl;
    private final String tokenUrl;
    private final String apiKey;
    private final String statusUrl;
    
    private String authorization;

    StreamingAnalyticsServiceV2(JsonObject service) {
        super(service);
        tokenUrl = StreamsRestUtils.getTokenUrl(credentials());
        apiKey = StreamsRestUtils.getServiceApiKey(credentials());
        statusUrl = jstring(credentials(), "v2_rest_url");
    }

    // Synchronized because it needs to read and possibly write two members
    // that are interdependent: authExpiryTime and authorization. Should be
    // fast enough without getting tricky: contention should be rare because
    // of the way we use this, and this should be fast compared to the network
    // I/O that typically follows using the returned authorization.
    @Override
    synchronized protected String getAuthorization() {
        if (System.currentTimeMillis() > authExpiryTime) {
            refreshAuthorization();
        }
        return authorization;
    }

    private void refreshAuthorization() {
        JsonObject response = StreamsRestUtils.getTokenResponse(tokenUrl, apiKey);
        if (null != response) {
            String accessToken = StreamsRestUtils.getToken(response);
            if (null != accessToken) {
            	authorization = RestUtils.createBearerAuth(accessToken);
                authExpiryTime = StreamsRestUtils.getTokenExpiryMillis(response);
            }
        }
    }

    @Override
    JsonObject getServiceStatus(CloseableHttpClient httpClient)
            throws IOException, IllegalStateException {
        JsonObject response = super.getServiceStatus(httpClient);
        if (null == jobSubmitUrl || null == buildsUrl) {
            setUrls(response);
        }
        return response;
    }
    
    @Override
    protected String getStatusUrl(CloseableHttpClient httpclient) throws IOException {
        return statusUrl;
    }

    @Override
    protected String getJobSubmitUrl(CloseableHttpClient httpclient, File bundle)
            throws IOException {
        if (null == jobSubmitUrl) {
            getServiceStatus(httpclient);
        }
        return jobSubmitUrl;
    }

    @Override
    protected String getJobSubmitUrl(JsonObject artifact)
            throws IOException {

        return jstring(artifact, "submit_job");
    }

    @Override
    protected String getBuildsUrl(CloseableHttpClient httpclient)
            throws IOException {
        if (null == buildsUrl) {
            getServiceStatus(httpclient);
        }
        return buildsUrl;
    }
    
    

    private synchronized void setUrls(JsonObject statusResponse)
            throws IllegalStateException {
        jobSubmitUrl = jstring(statusResponse, "jobs");
        // Builds URL is not public in response, kludge from jobs url
        if (!jobSubmitUrl.endsWith("/jobs")) {
            throw new IllegalStateException("Unexpected jobs URL: " + jobSubmitUrl);
        }
        buildsUrl = jobSubmitUrl.substring(0, jobSubmitUrl.length() - 4) + "builds";
    }

    @Override
    protected String getJobSubmitId() {
        return "id";
    }

    @Override
    protected JsonObject getBuild(String buildId, CloseableHttpClient httpclient) throws IOException {
        String buildURL = getBuildsUrl(httpclient) + "/"
            + URLEncoder.encode(buildId, StandardCharsets.UTF_8.name());
        HttpGet httpget = new HttpGet(buildURL);
        httpget.addHeader("Authorization", getAuthorization());

        return StreamsRestUtils.getGsonResponse(httpclient, httpget);
    }

    @Override
    protected JsonObject getBuildOutput(String buildId, String outputId,
            CloseableHttpClient httpclient)
            throws IOException {
        String buildOutputURL = getBuildsUrl(httpclient) + "/"
                + URLEncoder.encode(buildId, StandardCharsets.UTF_8.name())
                + "?output_id="
                + URLEncoder.encode(outputId, StandardCharsets.UTF_8.name());
        HttpGet httpget = new HttpGet(buildOutputURL);
        httpget.addHeader("Authorization", getAuthorization());

        return StreamsRestUtils.getGsonResponse(httpclient, httpget);
    }

    @Override
    protected JsonObject submitBuild(CloseableHttpClient httpclient,
            File archive, String buildName, String originator)
            throws IOException {
        String newBuildURL = getBuildsUrl(httpclient);
        
        newBuildURL = newBuildURL + "?originator=" +
            URLEncoder.encode(originator, StandardCharsets.UTF_8.name());
        
        HttpPost httppost = new HttpPost(newBuildURL);
        
        httppost.addHeader("Authorization", getAuthorization());

        JsonObject buildParams = new JsonObject();
        buildParams.addProperty("buildName", buildName);

        StringBody paramsBody= new StringBody(buildParams.toString(),
                ContentType.APPLICATION_JSON);

        FileBody archiveBody = new FileBody(archive,
                ContentType.create("application/zip"), archive.getName());

        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart("build_options", paramsBody)
                .addPart("archive_file", archiveBody).build();

        httppost.setEntity(reqEntity);
        JsonObject build = StreamsRestUtils.getGsonResponse(httpclient, httppost);
        return build;
    }

    /**
     * Submit the job from the built artifact.
     */
    @Override
    protected JsonObject submitBuildArtifact(CloseableHttpClient httpclient,
            JsonObject jobConfigOverlays, String submitUrl)
            throws IOException {
        HttpPost postArtifact = new HttpPost(submitUrl);
        postArtifact.addHeader("Authorization", getAuthorization());

        StringBody paramsBody = new StringBody(jobConfigOverlays.toString(),
                ContentType.APPLICATION_JSON);
        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart("job_options", paramsBody).build();
        postArtifact.setEntity(reqEntity);

        JsonObject jso = StreamsRestUtils.getGsonResponse(httpclient, postArtifact);
        TRACE.info("Streaming Analytics service (" + getName() + "): submit job response: " + jso.toString());
        return jso;
    }

    @Override
    AbstractStreamingAnalyticsConnection createStreamsConnection() throws IOException {
        return StreamingAnalyticsConnectionV2.of(this, service, false);
    }

    @Override
    protected List<File> downloadArtifacts(CloseableHttpClient httpclient, JsonArray artifacts) {
        final List<File> files = new ArrayList<>();
        for (JsonElement ae : artifacts) {
            JsonObject artifact = ae.getAsJsonObject();
            if (!artifact.has("download"))
                continue;
            
            String name = jstring(artifact, "name");
            String url = jstring(artifact, "download");
           
            // Don't fail the submit if we fail to download the sab(s).
            try {
                File target = new File(name);
                StreamsRestUtils.getFile(Executor.newInstance(httpclient), getAuthorization(), url, target);
                files.add(target);
            } catch (IOException e) {
                TRACE.warning("Failed to download sab: " + name + " : " + e.getMessage());
            }
        }
        return files;
    }
    
    @Override
    public Result<List<File>, JsonObject> build(File archive,
            String buildName, JsonObject buildConfig) throws IOException {
        
        JsonObject metrics = new JsonObject();
        metrics.addProperty(SubmissionResultsKeys.SUBMIT_ARCHIVE_SIZE, archive.length());
            
        CloseableHttpClient httpclient = RestUtils.createHttpClient();
        try {
            // Set up the build name
            if (null == buildName) {
                buildName = "build";
            }
            buildName = getSPLCompatibleName(buildName) + "_" + randomHex(16);
            buildName = URLEncoder.encode(buildName, StandardCharsets.UTF_8.name());
            
            String originator = null;
            if (buildConfig != null) {
                originator = jstring(buildConfig, "originator");
            }
            if (originator == null)
                originator = DEFAULT_ORIGINATOR;
                      
            // Perform initial post of the archive
            TRACE.info("Streaming Analytics service (" + serviceName + "): submitting build " + buildName + " originator " + originator);
            final long startUploadTime = System.currentTimeMillis();
            JsonObject buildSubmission = submitBuild(httpclient, archive, buildName, originator);
            final long endUploadTime = System.currentTimeMillis();
            metrics.addProperty(SubmissionResultsKeys.SUBMIT_UPLOAD_TIME, (endUploadTime - startUploadTime));
            
            String buildId = jstring(buildSubmission, "id");
            String outputId = jstring(buildSubmission, "output_id");

            // Loop until built
            final long startBuildTime = endUploadTime;
            long lastCheckTime = endUploadTime;
            JsonObject buildStatus = getBuild(buildId, httpclient);  
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
                    buildStatus = getBuild(buildId, httpclient); 
                    status = jstring(buildStatus, "status");
                    continue;
                } 
                // The remaining possible states are 'failed', 'timeout', 'canceled', 'canceling', and 'unknown', none of which can lead to a state of 'built', so we throw an error.
                else {
                    TRACE.severe("Streaming Analytics service (" + serviceName + "): The submitted archive " + archive.getName() + " failed to build with status " + status + ".");
                    JsonObject output = getBuildOutput(buildId, outputId, httpclient);
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
            
            final long startDownloadSabTime = System.currentTimeMillis();
            final List<File> files = downloadArtifacts(httpclient, artifacts);
            final long endDownloadSabTime = System.currentTimeMillis();
            metrics.addProperty(SubmissionResultsKeys.DOWNLOAD_SABS_TIME,
                        (endDownloadSabTime - startDownloadSabTime));
            
            Result<List<File>,JsonObject> result = new ResultImpl<>(true, buildName,
                    ()->files, new JsonObject());
            result.getRawResult().add(SubmissionResultsKeys.SUBMIT_METRICS, metrics);
            result.getRawResult().add(SubmissionResultsKeys.BUILD_STATUS, buildStatus);
            
            JsonArray jsonFiles = new JsonArray();
            for (File f : files)
                jsonFiles.add(new JsonPrimitive(f.getAbsolutePath()));
            
            result.getRawResult().add("sabs", jsonFiles);
            
            if (files.size() == 1)
                result.getRawResult().addProperty(SubmissionResultsKeys.BUNDLE_PATH,
                        files.get(0).getAbsolutePath());
            
            return result;
        } finally {
            httpclient.close();
        }
    }

}
