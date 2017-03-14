/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.copyJobConfigOverlays;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.streaminganalytics.RestUtils;

class BuildServiceRemoteRESTWrapper {
	
    private JsonObject credentials;
    private JsonObject service;
	
	BuildServiceRemoteRESTWrapper(JsonObject service){
	    JsonObject credentials = object(service,  "credentials");
	    this.credentials = credentials;
	    this.service = service;
    }
	

	void remoteBuildAndSubmit(JsonObject submission, File archive) throws ClientProtocolException, IOException {
	    JsonObject deploy = DeployKeys.deploy(submission);
	    JsonObject graph = object(submission, "graph");
	    String graphBuildName = jstring(graph, "name");
	    
		CloseableHttpClient httpclient = HttpClients.createDefault();
		try {
			String serviceName = jstring(service, "name");
			RemoteContext.REMOTE_LOGGER.info("Streaming Analytics service (" + serviceName + "): Checking status");
			RestUtils.checkInstanceStatus(httpclient, this.service);

			String apiKey = RestUtils.getAPIKey(credentials);

			// Perform initial post of the archive
			String buildName = graphBuildName + "_" + randomHex(16);
			buildName = URLEncoder.encode(buildName, StandardCharsets.UTF_8.name());
			RemoteContext.REMOTE_LOGGER.info("Streaming Analytics service (" + serviceName + "): submitting build " + buildName);
			JsonObject jso = doUploadBuildArchivePost(httpclient, apiKey, archive, buildName);

			JsonObject build = object(jso, "build");
			String buildId = jstring(build, "id");
			String outputId = jstring(build, "output_id");

			// Loop until built
			String status = buildStatusGet(buildId, httpclient, apiKey);
			while (!status.equals("built")) {
				// 'building', 'notBuilt', and 'waiting' are all states which can eventualy result in 'built'
				// sleep and continue to monitor
				if (status.equals("building") || status.equals("notBuilt") || status.equals("waiting")) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					status = buildStatusGet(buildId, httpclient, apiKey);
					continue;
				} 
				// The remaining possible states are 'failed', 'timeout', 'canceled', 'canceling', and 'unknown', none of which can lead to a state of 'built', so we throw an error.
				else {
					RemoteContext.REMOTE_LOGGER.severe("Streaming Analytics service (" + serviceName + "): The submitted archive " + archive.getName() + " failed to build with status " + status + ".");
					JsonObject output = getBuildOutput(buildId, outputId, httpclient, apiKey);
					String strOutput = "";
					if (output != null)
						strOutput = prettyPrintOutput(output);
					throw new IllegalStateException("Error submitting archive for compilation: \n" + strOutput);
				}
			}

			// Now perform archive put
			build = getBuild(buildId, httpclient, apiKey);

			JsonArray artifacts = array(build, "artifacts");
			if (artifacts == null || artifacts.size() == 0) {
				throw new IllegalStateException("No artifacts associated with build " + buildId);
			}

			// TODO: support multiple artifacts associated with a single build.
			String artifactId = jstring(artifacts.get(0).getAsJsonObject(), "id");
			RemoteContext.REMOTE_LOGGER.info("Streaming Analytics service (" + serviceName + "): submitting job request.");
			JsonObject response = doSubmitJobFromBuildArtifactPut(httpclient, deploy, apiKey, artifactId);
			
			// Pass back to Python
			final JsonObject submissionResult = GsonUtilities.objectCreate(submission, RemoteContext.SUBMISSION_RESULTS);
			GsonUtilities.addAll(submissionResult, response);
		} finally {
			httpclient.close();

		}
	}
	
	/**
	 * Submit the job from the built artifact.
	 */
	private JsonObject doSubmitJobFromBuildArtifactPut(CloseableHttpClient httpclient,
	        JsonObject deploy,
			String apiKey, String artifactId) throws ClientProtocolException, IOException{
		String putURL = getBuildsURL() + "?artifact_id=" + URLEncoder.encode(artifactId, StandardCharsets.UTF_8.name());
		HttpPut httpput = new HttpPut(putURL);
        httpput.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
        httpput.addHeader("Authorization", apiKey);
        httpput.addHeader("content-type", ContentType.APPLICATION_JSON.getMimeType());
        
        JsonObject jobConfigOverlays = copyJobConfigOverlays(deploy);       
        
        StringEntity params =new StringEntity(jobConfigOverlays.toString(),
                ContentType.APPLICATION_JSON);    
        httpput.setEntity(params);
       
        JsonObject jso = RestUtils.getGsonResponse(httpclient, httpput);
        
        String serviceName = jstring(service, "name");
        RemoteContext.REMOTE_LOGGER.info("Streaming Analytics service (" + serviceName + "): submit job response: " + jso.toString());
		return jso;
	}
	
	private String prettyPrintOutput(JsonObject output) {
		StringBuilder sb = new StringBuilder();
		for(JsonElement messageElem : array(output, "output")){
			JsonObject message = messageElem.getAsJsonObject();
			sb.append(message.get("message_text") + "\n");
		}
		return sb.toString();
	}

	private JsonObject doUploadBuildArchivePost(CloseableHttpClient httpclient,
						    String apiKey, File archive, String buildName) throws ClientProtocolException, IOException{
	    String newBuildURL = getBuildsURL() + "?build_name=" + URLEncoder.encode(buildName, StandardCharsets.UTF_8.name());
		HttpPost httppost = new HttpPost(newBuildURL);
        httppost.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
        httppost.addHeader("Authorization", apiKey);
        
		FileBody archiveBody = new FileBody(archive,
        		ContentType.create("application/zip"));
        
        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart(archive.getName(), archiveBody).build();
        
        httppost.setEntity(reqEntity);
        JsonObject jso = RestUtils.getGsonResponse(httpclient, httppost);
        return jso;
	}
	
	/**
	 * Retrieves the status of the build.
	 * @param buildId
	 * @param httpclient
	 * @param apiKey
	 * @return The status of the build associated with *buildId* as a String.
	 * @throws IOException 
	 * @throws ClientProtocolException 
	 */
	private String buildStatusGet(String buildId, CloseableHttpClient httpclient,
			String apiKey) throws ClientProtocolException, IOException{
        JsonObject build = getBuild(buildId, httpclient, apiKey);   
		if(build != null)
			return jstring(build, "status");
		else
			return null;
	}
	
	private JsonObject getBuild(String buildId, CloseableHttpClient httpclient,
			String apiKey) throws ClientProtocolException, IOException{
		String buildURL = getBuildsURL() + "?build_id=" + URLEncoder.encode(buildId, StandardCharsets.UTF_8.name());
		HttpGet httpget = new HttpGet(buildURL);
        httpget.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
        httpget.addHeader("Authorization", apiKey);
		
		JsonObject response = RestUtils.getGsonResponse(httpclient, httpget);
		// Get the correct build
		JsonObject build = null;
		JsonArray builds = array(response, "builds");
		for (JsonElement iterBuildElem : builds) {
			JsonObject iterBuild = iterBuildElem.getAsJsonObject();
			if (jstring(iterBuild, "id").equals(buildId))
				build = iterBuild;
		}
		return build;
	}
	
	private JsonObject getBuildOutput(String buildId, String outputId, CloseableHttpClient httpclient,
			String apiKey) throws ClientProtocolException, IOException{
		String buildOutputURL = getBuildsURL() + "?build_id=" + URLEncoder.encode(buildId, StandardCharsets.UTF_8.name())
				+ "&output_id=" + URLEncoder.encode(outputId, StandardCharsets.UTF_8.name());
		System.out.println(buildOutputURL);
		HttpGet httpget = new HttpGet(buildOutputURL);
		httpget.addHeader("Authorization", apiKey);
        httpget.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
		
		JsonObject response = RestUtils.getGsonResponse(httpclient, httpget);
		for(JsonElement outputElem : array(response, "builds")){
			JsonObject output = outputElem.getAsJsonObject();
			if(jstring(output, "id").equals(buildId))
				return output;
		}
		
		return null;
	}
	
	private String randomHex(int length){
		char[] hexes = "0123456789ABCDEF".toCharArray();
		Random r = new Random();
		String name = "";
		for(int i = 0; i < length; i++){
			name += String.valueOf((hexes[r.nextInt(hexes.length)]));
		}
		return name;
	}
	
	private String getBuildsURL(){
		String buildURL = jstring(credentials, "jobs_path").replace("jobs", "builds");
		return jstring(credentials, "rest_url") + buildURL;
	}
}

