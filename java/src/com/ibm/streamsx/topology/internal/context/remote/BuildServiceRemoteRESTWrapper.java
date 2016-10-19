package com.ibm.streamsx.topology.internal.context.remote;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Random;

import javax.xml.bind.DatatypeConverter;

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

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.internal.context.RemoteContexts;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.json.java.JSONArray;

public class BuildServiceRemoteRESTWrapper {
	
	JSONObject credentials;
	
	public BuildServiceRemoteRESTWrapper(JSONObject credentials){
		this.credentials = credentials;
	}
	
	public void remoteBuildAndSubmit(File archive) throws ClientProtocolException, IOException{
		CloseableHttpClient httpclient = HttpClients.createDefault();
        
        String apiKey = getAPIKey((String)credentials.get("userid"), (String)credentials.get("password"));
        
        
        // Perform initial post of the archive
        JsonObject jso = doArchivePost(httpclient, apiKey, archive);
        
        JsonObject build = GsonUtilities.object(jso, "build");
        String buildId = GsonUtilities.jstring(build, "id");
        String outputId = GsonUtilities.jstring(build,  "output_id");
        
        // Loop until built
        String status = "";
        while(!status.equals("built")){
        	status = buildStatusGet(buildId, httpclient, apiKey);
        	if(status.equals("building")){
        		continue;
        	}
        	else if(status.equals("failed")){
        		JsonObject output = getBuildOutput(buildId, outputId, httpclient, apiKey);
        		String strOutput = "";
        		if(output!=null)
        			strOutput = prettyPrintOutput(output);
        		throw new IllegalStateException("Error submitting archive for compilation: \n"
        				+ strOutput);
        	}
        }
        
        // Now perform archive put
        build = getBuild(buildId, httpclient, apiKey);
        
        JsonArray artifacts = GsonUtilities.array(build, "artifacts");
        if(artifacts.size() == 0){
        	throw new IllegalStateException("No artifacts associated with build " + buildId);
        }
        
        // TODO: support multiple artifacts associated with a single build.
        String artifactId = GsonUtilities.jstring(artifacts.get(0).getAsJsonObject(), "id");
        doArchivePut(httpclient, apiKey, artifactId);
	}
	
	private JsonObject doArchivePut(CloseableHttpClient httpclient,
			String apiKey, String artifactId) throws ClientProtocolException, IOException{
		String putURL = getBuildsURL(credentials) + "?artifact_id=" + artifactId;
		HttpPut httpput = new HttpPut(putURL);
        httpput.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
        httpput.addHeader("Authorization", apiKey);
        //httpput.addHeader("Content-Length", "2");
        httpput.addHeader("content-type", "application/json");
        
        StringEntity params =new StringEntity("{}","UTF-8");    
        httpput.setEntity(params);
       
        //System.out.println(httppost.getAllHeaders()[1]);
        JsonObject jso = RemoteContexts.getGsonResponse(httpclient, httpput);
		return jso;
	}
	
	private String prettyPrintOutput(JsonObject output) {
		StringBuffer sb = new StringBuffer();
		for(JsonElement messageElem : GsonUtilities.array(output, "output")){
			JsonObject message = messageElem.getAsJsonObject();
			sb.append(message.get("message_text") + "\n");
		}
		return sb.toString();
	}

	private JsonObject doArchivePost(CloseableHttpClient httpclient,
			String apiKey, File archive) throws ClientProtocolException, IOException{
		String newBuildURL = getBuildsURL(credentials) + "?build_name=" + newBuildName(16);
		HttpPost httppost = new HttpPost(newBuildURL);
        httppost.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
        httppost.addHeader("Authorization", apiKey);
        
        @SuppressWarnings("deprecation")
		FileBody archiveBody = new FileBody(archive,
        		"application/zip");
        
        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart(archive.getName(), archiveBody).build();
        
        httppost.setEntity(reqEntity);
        //System.out.println(httppost.getAllHeaders()[1]);
        JsonObject jso = RemoteContexts.getGsonResponse(httpclient, httppost);
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
			return GsonUtilities.jstring(build, "status");
		else
			return null;
	}
	
	private JsonObject getBuild(String buildId, CloseableHttpClient httpclient,
			String apiKey) throws ClientProtocolException, IOException{
		String buildURL = getBuildsURL(credentials) + "?build_id=" + buildId;
		HttpGet httpget = new HttpGet(buildURL);
        httpget.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
        httpget.addHeader("Authorization", apiKey);
		
		JsonObject response = RemoteContexts.getGsonResponse(httpclient, httpget);
		// Get the correct build
		JsonObject build = null;
		JsonArray builds = GsonUtilities.array(response, "builds");
		for (JsonElement iterBuildElem : builds) {
			JsonObject iterBuild = iterBuildElem.getAsJsonObject();
			if (GsonUtilities.jstring(iterBuild, "id").equals(buildId))
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
		
		JsonObject response = RemoteContexts.getGsonResponse(httpclient, httpget);
		for(JsonElement outputElem : GsonUtilities.array(response, "builds")){
			JsonObject output = outputElem.getAsJsonObject();
			if(GsonUtilities.jstring(output, "id").equals(buildId))
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
	
	private String getAPIKey(String userid, String password) throws UnsupportedEncodingException{
		String api_creds = userid + ":" + password;
		String apiKey = "Basic "
						+ DatatypeConverter.printBase64Binary(api_creds
								.getBytes("UTF-8"));			
		return apiKey;
	}

	
	private String getBuildsURL(JSONObject credentials){
		String buildURL = ((String) credentials.get("jobs_path")).replace("jobs", "builds");
		return credentials.get("rest_url") + buildURL;
	}
}

