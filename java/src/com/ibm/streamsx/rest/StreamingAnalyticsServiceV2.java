/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest;

import static com.ibm.streamsx.rest.StreamsRestUtils.TRACE;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;

import com.google.gson.JsonObject;

class StreamingAnalyticsServiceV2 extends AbstractStreamingAnalyticsService {

    private long authExpiryTime = -1L;

    private String jobSubmitUrl;
    private String buildsUrl;
    private final String tokenUrl;
    private final String apiKey;
    private final String statusUrl;

    StreamingAnalyticsServiceV2(JsonObject service) {
        super(service);
        tokenUrl = StreamsRestUtils.getTokenUrl(credentials);
        apiKey = StreamsRestUtils.getServiceApiKey(credentials);
        statusUrl = jstring(credentials, "v2_rest_url");
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
                setAuthorization(StreamsRestUtils.createBearerAuth(accessToken));
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
    protected JsonObject getBuild(String buildId, CloseableHttpClient httpclient,
            String authorization) throws IOException {
        String buildURL = getBuildsUrl(httpclient) + "/"
            + URLEncoder.encode(buildId, StandardCharsets.UTF_8.name());
        HttpGet httpget = new HttpGet(buildURL);
        httpget.addHeader("Authorization", authorization);

        return StreamsRestUtils.getGsonResponse(httpclient, httpget);
    }

    @Override
    protected JsonObject getBuildOutput(String buildId, String outputId,
            CloseableHttpClient httpclient, String authorization)
            throws IOException {
        String buildOutputURL = getBuildsUrl(httpclient) + "/"
                + URLEncoder.encode(buildId, StandardCharsets.UTF_8.name())
                + "?output_id="
                + URLEncoder.encode(outputId, StandardCharsets.UTF_8.name());
        HttpGet httpget = new HttpGet(buildOutputURL);
        httpget.addHeader("Authorization", authorization);

        return StreamsRestUtils.getGsonResponse(httpclient, httpget);
    }

    @Override
    protected JsonObject submitBuild(CloseableHttpClient httpclient,
            String authorization, File archive, String buildName)
            throws IOException {
        String newBuildURL = getBuildsUrl(httpclient);
        HttpPost httppost = new HttpPost(newBuildURL);
        httppost.addHeader("Authorization", authorization);

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
    protected JsonObject submitBuildArtifact(CloseableHttpClient httpclient,
            JsonObject jobConfigOverlays, String authorization, String submitUrl)
            throws IOException {
        HttpPost postArtifact = new HttpPost(submitUrl);
        postArtifact.addHeader("Authorization", authorization);

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
        return StreamingAnalyticsConnectionV2.of(service, getAuthorization(),
                authExpiryTime, false);
    }

}
