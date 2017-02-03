/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.getJobConfigOverlays;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.io.IOException;
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
import com.ibm.streamsx.topology.internal.streaminganalytics.RestUtils;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

class BuildServiceRemoteRESTWrapper {
	
    private JsonObject credentials;
	
	BuildServiceRemoteRESTWrapper(JsonObject credentials){
		this.credentials = credentials;
    }
	

	void remoteBuildAndSubmit(JsonObject deploy, File archive) throws ClientProtocolException, IOException {
		CloseableHttpClient httpclient = HttpClients.createDefault();
		try {
			JsonObject serviceg = VcapServices.getVCAPService(deploy);
			String serviceName = serviceg.get("name").toString();

			RemoteContext.REMOTE_LOGGER.info("Streaming Analytics Service (" + serviceName + "): Checking status :" + serviceName);
			RestUtils.checkInstanceStatus(httpclient, this.credentials);

			String apiKey = RestUtils.getAPIKey(credentials);

			// Perform initial post of the archive
			String buildName = newBuildName(16);
			RemoteContext.REMOTE_LOGGER.info("Streaming Analytics Service: Submitting build : \"" + buildName + "\" to " + serviceg.get("name"));
			JsonObject jso = doUploadBuildArchivePost(httpclient, apiKey, archive, buildName);

			JsonObject build = object(jso, "build");
			String buildId = jstring(build, "id");
			String outputId = jstring(build, "output_id");

			// Loop until built
			String status = "";
			while (!status.equals("built")) {
				status = buildStatusGet(buildId, httpclient, apiKey);
				if (status.equals("building")) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					continue;
				} else if (status.equals("failed")) {
					RemoteContext.REMOTE_LOGGER.severe("Streaming Analytics Service (" + serviceName + "): The submitted archive " + archive.getName() + " failed to build.");
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
			RemoteContext.REMOTE_LOGGER.info("Streaming Analytics Service (" + serviceName + "): submiting job request.");
			doSubmitJobFromBuildArtifactPut(httpclient, deploy, apiKey, artifactId);
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
		String putURL = getBuildsURL(credentials) + "?artifact_id=" + artifactId;
		HttpPut httpput = new HttpPut(putURL);
        httpput.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
        httpput.addHeader("Authorization", apiKey);
        httpput.addHeader("content-type", ContentType.APPLICATION_JSON.getMimeType());
        
        JsonObject jobConfigOverlays = getJobConfigOverlays(deploy);       
        
        StringEntity params =new StringEntity(jobConfigOverlays.toString(),
                ContentType.APPLICATION_JSON);    
        httpput.setEntity(params);
       
        JsonObject jso = RestUtils.getGsonResponse(httpclient, httpput);
        
        JsonObject serviceg = VcapServices.getVCAPService(deploy);
        String serviceName = serviceg.get("name").toString();
        RemoteContext.REMOTE_LOGGER.info("Streaming Analytics Service(" + serviceName + "): submit job response: " + jso.toString());
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
	    String newBuildURL = getBuildsURL(credentials) + "?build_name=" + buildName;
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
		String buildURL = getBuildsURL(credentials) + "?build_id=" + buildId;
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
		String buildOutputURL = getBuildsURL(credentials) + "?build_id=" + buildId
				+ "&output_id=" + outputId;
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
	
	private String newBuildName(int length){
		char[] hexes = "0123456789ABCDEF".toCharArray();
		Random r = new Random();
		String name = "";
		for(int i = 0; i < length; i++){
			name += String.valueOf((hexes[r.nextInt(hexes.length)]));
		}
		return name;
	}
	
	private String getBuildsURL(JsonObject credentials){
		String buildURL = jstring(credentials, "jobs_path").replace("jobs", "builds");
		return jstring(credentials, "rest_url") + buildURL;
	}
}

