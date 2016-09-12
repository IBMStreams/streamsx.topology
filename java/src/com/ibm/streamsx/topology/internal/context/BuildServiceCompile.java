package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;

public class BuildServiceCompile extends ZippedToolkitStreamsContext {

	@Override
	public Future<File> submit(Topology app, Map<String, Object> config) throws Exception {  
		String namespace = (String) app.builder().json().get("namespace");
		String mainComposite = (String) app.builder().json().get("name");
		
        File zippedArchive = super.submit(app, config).get();
        
        File bundle = doRemoteCompile(namespace, mainComposite, zippedArchive);
        
        return new CompletedFuture<File>(bundle);
	}
	
	private File doRemoteCompile(String namespace, String mainComposite, 
			File zippedArchive) throws ClientProtocolException, IOException {
		String bundleName = namespace + "." + mainComposite + ".sab";
		String archiveName = zippedArchive.getName();
		HttpClient client = HttpClientBuilder.create().build();
		
		File archiveFile = new File(archiveName);
		HttpPost postJobWithConfig = new HttpPost(getBuildFarmURL());
        postJobWithConfig.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());

        FileBody bundleBody = new FileBody(archiveFile, 
        		ContentType.APPLICATION_OCTET_STREAM, archiveName);
        
        StringBody configBody = new StringBody(createBuildConfig(namespace, mainComposite),
        		ContentType.TEXT_PLAIN);

        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart("inputfile", bundleBody).addPart("buildconfig", configBody)
                .build();

        postJobWithConfig.setEntity(reqEntity);
        
        File bundle = doPostAndRetreiveFile((CloseableHttpClient) client, 
        		postJobWithConfig, bundleName);       
        return bundle;
	}

	private String getBuildFarmURL(){
		// For the moment, just fetch a static public build farm url. 
		// TODO: grab from VCAP file
		return "https://streamsbuildservice.mybluemix.net/api/build/compile/spltoolkit";
	}

	private String createBuildConfig(String namespace, String mainComposite){
		String config = "{"
							+ "\"namespace\":\"" + namespace + "\","
							+ "\"mainComposite\":\"" + mainComposite + "\""
							+ ",\"streamsVersion\":\"4.2\""
							+ ",\"pythonVersion\":\"3.5\""
							+ ",\"timeout\":100"
					  + "}";
		return config;
	}
	
	
    private File doPostAndRetreiveFile(CloseableHttpClient httpClient,
            HttpRequestBase request, String bundleName) 
            		throws ClientProtocolException, IOException {
        request.addHeader("accept",
                ContentType.APPLICATION_JSON.getMimeType());

        File bundle;
        CloseableHttpResponse response = AnalyticsServiceStreamsContext
        		.executeAndHandleErrors(httpClient, request);
        HttpEntity entity = response.getEntity();
        
        try { 	
            InputStream is = null;
            FileOutputStream fos = null;
            byte[] buffer = new byte[1024];

            try{
            	bundle = new File(bundleName);
            	is = entity.getContent();
            	fos = new FileOutputStream(bundle);

            	// Write the bundle in chunks.
            	for(int length; (length = is.read(buffer)) > 0;){
            		fos.write(buffer, 0, length);
            	}
            }
            finally{
            	if(is != null)  try {is.close();}  catch(IOException e) {
            		throw e;
            	}
            	if(fos != null) try {fos.close();} catch(IOException e) {
            		throw e;
            	}
            }

        } finally {
            response.close();
        }
        return bundle;
    }	
}
