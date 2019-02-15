package com.ibm.streamsx.rest.build;

import java.io.IOException;

import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import com.google.gson.JsonObject;

class StreamsBuildService extends AbstractConnection implements BuildService {
	
	private String endpoint;
	private String authorization;

	public StreamsBuildService(String endpoint, String bearerToken) {
		super(false);
		this.endpoint = endpoint;
		setAuthorization(bearerToken);
	}
	
	private void setAuthorization(String bearerToken) {
		authorization = StreamsRestUtils.createBearerAuth(bearerToken);
	}
	
	@Override
	String getAuthorization() {
		return authorization;
	}
	
	
	
	@Override
	public void allowInsecureHosts() {
		this.executor = StreamsRestUtils.createExecutor(true);	
	}

	@Override
	public Build createBuild(String name, JsonObject parameters) throws IOException {
		
		JsonObject buildParams = new JsonObject();

		buildParams.addProperty("type", "application");
		buildParams.addProperty("incremental", false);		
		if (name != null)
			buildParams.addProperty("name", name);
		
		Request post = Request.Post(endpoint)	      
		    .addHeader("Authorization", getAuthorization())
		    .bodyString(buildParams.toString(),
		                ContentType.APPLICATION_JSON);
		
		Build build = Build.create(this, this, StreamsRestUtils.requestGsonResponse(executor, post));
		return build;
	}

}
