package com.ibm.streamsx.topology.internal.context;

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
import com.ibm.json.java.JSONArray;

public class BuildServiceRESTWrapper {
	
	JSONObject credentials;
	
	public BuildServiceRESTWrapper(JSONObject credentials){
		this.credentials = credentials;
	}
	
	public JSONObject remoteBuildAndSubmit(File archive) throws ClientProtocolException, IOException{
		CloseableHttpClient httpclient = HttpClients.createDefault();
        
        String apiKey = getAPIKey((String)credentials.get("userid"), (String)credentials.get("password"));
        
        
        // Perform initial post of the archive
        JSONObject jso = doArchivePost(httpclient, apiKey, archive);
        
        JSONObject build = (JSONObject)jso.get("build");
        String buildId = (String)build.get("id");
        String outputId = (String)build.get("output_id");
        
        // Loop until built
        String status = "";
        while(!status.equals("built")){
        	status = buildStatusGet(buildId, httpclient, apiKey);
        	if(status.equals("building")){
        		continue;
        	}
        	else if(status.equals("failed")){
        		JSONObject output = getBuildOutput(buildId, outputId, httpclient, apiKey);
        		String strOutput = "";
        		if(output!=null)
        			strOutput = prettyPrintOutput(output);
        		throw new IllegalStateException("Error submitting archive for compilation: \n"
        				+ strOutput);
        	}
        }
        
        // Now perform archive put
        build = getBuild(buildId, httpclient, apiKey);
        
        JSONArray artifacts = ((JSONArray)build.get("artifacts"));
        if(artifacts.size() == 0){
        	throw new IllegalStateException("No artifacts associated with build " + buildId);
        }
        
        // TODO: support multiple artifacts associated with a single build.
        String artifactId = (String)((JSONObject)artifacts.get(0)).get("id");
        return doArchivePut(httpclient, apiKey, artifactId);
	}
	
	private JSONObject doArchivePut(CloseableHttpClient httpclient,
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
        JSONObject jso = Contexts.getJsonResponse(httpclient, httpput);
		return jso;
	}
	
	private String prettyPrintOutput(JSONObject output) {
		StringBuffer sb = new StringBuffer();
		for(Object messageObj : (JSONArray)output.get("output")){
			JSONObject message = (JSONObject)messageObj;
			sb.append(message.get("message_text") + "\n");
		}
		return sb.toString();
	}

	private JSONObject doArchivePost(CloseableHttpClient httpclient,
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
        JSONObject jso = Contexts.getJsonResponse(httpclient, httppost);
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
        JSONObject build = getBuild(buildId, httpclient, apiKey);   
		if(build != null)
			return (String)build.get("status");  
		else
			return null;
	}
	
	private JSONObject getBuild(String buildId, CloseableHttpClient httpclient,
			String apiKey) throws ClientProtocolException, IOException{
		String buildURL = getBuildsURL(credentials) + "?build_id=" + buildId;
		HttpGet httpget = new HttpGet(buildURL);
        httpget.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
        httpget.addHeader("Authorization", apiKey);
		
		JSONObject response = Contexts.getJsonResponse(httpclient, httpget);
		// Get the correct build
		JSONObject build = null;
		JSONArray builds = (JSONArray) response.get("builds");
		for (Object iterBuildObj : builds) {
			JSONObject iterBuild = (JSONObject) iterBuildObj;
			if (((String) iterBuild.get("id")).equals(buildId))
				build = iterBuild;
		}
		return build;
	}
	
	private JSONObject getBuildOutput(String buildId, String outputId, CloseableHttpClient httpclient,
			String apiKey) throws ClientProtocolException, IOException{
		String buildOutputURL = getBuildsURL(credentials) + "?build_id=" + buildId
				+ "&output_id=" + outputId;
		System.out.println(buildOutputURL);
		HttpGet httpget = new HttpGet(buildOutputURL);
		httpget.addHeader("Authorization", apiKey);
        httpget.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
		
		JSONObject response = Contexts.getJsonResponse(httpclient, httpget);
		for(Object outputObj : (JSONArray)response.get("builds")){
			JSONObject output = (JSONObject)outputObj;
			if(((String)output.get("id")).equals(buildId))
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
