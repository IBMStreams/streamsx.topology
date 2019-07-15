package com.ibm.streamsx.rest.build;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.IOException;
import java.util.function.Function;

import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.internal.ICP4DAuthenticator;
import com.ibm.streamsx.rest.internal.RestUtils;

class StreamsBuildService extends AbstractConnection implements BuildService {
    
    public static BuildService of(ICP4DAuthenticator authenticator, JsonObject serviceDefinition, boolean verify) throws IOException {
        
        String buildServiceEndpoint = jstring(object(serviceDefinition, "connection_info"), "serviceBuildEndpoint");
        return new StreamsBuildService(buildServiceEndpoint, authenticator, verify);
    }
	
	private String endpoint;
	private Function<Executor, String> authenticator;

	private StreamsBuildService(String endpoint, Function<Executor, String> authenticator, boolean verify) {
		super(!verify);
		this.endpoint = endpoint;
		this.authenticator = authenticator;
	}
	
	@Override
	String getAuthorization() {
		return authenticator.apply(getExecutor());
	}
	
	@Override
	public void allowInsecureHosts() {
		this.executor = RestUtils.createExecutor(true);	
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
