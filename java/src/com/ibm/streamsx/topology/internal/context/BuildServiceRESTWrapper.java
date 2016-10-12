package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Random;

import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
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
	
	public String remoteBuildAndSubmit(File archive) throws ClientProtocolException, IOException{
		CloseableHttpClient httpclient = HttpClients.createDefault();
        
        String apiKey = getAPIKey((String)credentials.get("userid"), (String)credentials.get("password"));
        
        
        // Perform initial post of the archive
        JSONObject jso = doArchivePost(httpclient, apiKey, archive);
        
        JSONObject build = (JSONObject)jso.get("build");
        String buildId = (String)build.get("id");
        while(true){
        	System.out.println(buildStatusGet(buildId, httpclient, apiKey));
        }
   
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
        JSONObject jso = RemoteContexts.getJsonResponse(httpclient, httppost);
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
		String buildURL = getBuildsURL(credentials);
		HttpGet httpget = new HttpGet(buildId);
        httpget.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
        httpget.addHeader("Authorization", apiKey);
		
        JSONObject response = RemoteContexts.getJsonResponse(httpclient, httpget);
        
        // Get the correct build
		JSONObject build = null;
		JSONArray builds = (JSONArray) response.get("builds");
		for(Object iterBuildObj : builds){
			JSONObject iterBuild = (JSONObject) iterBuildObj;
			if((String)iterBuild.get("id") == buildId)
				build = iterBuild;
		}
		return (String)build.get("status");  
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
