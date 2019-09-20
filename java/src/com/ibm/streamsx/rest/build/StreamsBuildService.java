package com.ibm.streamsx.rest.build;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.IOException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.internal.RestUtils;

import com.ibm.streamsx.rest.internal.ICP4DAuthenticator;
import com.ibm.streamsx.rest.internal.StandaloneAuthenticator;
import com.ibm.streamsx.topology.internal.streams.Util;

class StreamsBuildService extends AbstractConnection implements BuildService {
    
    private static final String STREAMS_REST_BUILDS = "/streams/rest/builds";

    public static BuildService of(Function<Executor,String> authenticator, JsonObject serviceDefinition, boolean verify) throws IOException {
        
        String buildServiceEndpoint = jstring(object(serviceDefinition, "connection_info"), "serviceBuildEndpoint");
        if (authenticator instanceof StandaloneAuthenticator) {
            if (buildServiceEndpoint == null) {
                buildServiceEndpoint = Util.getenv(Util.STREAMS_BUILD_URL);
            }
            if (!buildServiceEndpoint.endsWith(STREAMS_REST_BUILDS) &&
                    authenticator instanceof StandaloneAuthenticator) {
                // URL was user-provided root of service, add the path
                URL url = new URL(buildServiceEndpoint);
                URL buildsUrl = new URL(url.getProtocol(), url.getHost(), url.getPort(), STREAMS_REST_BUILDS);
                buildServiceEndpoint = buildsUrl.toExternalForm();
            }
        }
        return new StreamsBuildService(buildServiceEndpoint, authenticator, verify);
    }

    public static BuildService of(Function<Executor,String> authenticator, String buildServiceEndpoint, boolean verify) throws IOException {

        if (buildServiceEndpoint == null) {
            buildServiceEndpoint = Util.getenv(Util.STREAMS_BUILD_URL);
            if (!buildServiceEndpoint.endsWith(STREAMS_REST_BUILDS) &&
                    authenticator instanceof StandaloneAuthenticator) {
                // URL was user-provided root of service, add the path
                URL url = new URL(buildServiceEndpoint);
                URL buildsUrl = new URL(url.getProtocol(), url.getHost(), url.getPort(), STREAMS_REST_BUILDS);
                buildServiceEndpoint = buildsUrl.toExternalForm();
            }
        }
        return new StreamsBuildService(buildServiceEndpoint, authenticator, verify);
    }
	
	private static final String TOOLKITS_RESOURCE_NAME = "toolkits";

	private String endpoint;
	private String authorization;
	private String toolkitsUrl;
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

	String getToolkitsURL() throws IOException {
		if (toolkitsUrl == null) {
			String resourcesUrl = endpoint;
			if (resourcesUrl.endsWith("/builds")) {
				resourcesUrl = resourcesUrl.replaceFirst("builds$", "resources");
			}
			// Query the resourcesUrl to find the resources URL
			String response = getResponseString(resourcesUrl);
			ResourcesArray resources = new GsonBuilder()
				.excludeFieldsWithoutExposeAnnotation()
				.create().fromJson(response, ResourcesArray.class);
			for (Resource resource : resources.resources) {
				if (TOOLKITS_RESOURCE_NAME.equals(resource.name)) {
					toolkitsUrl = resource.resource;
					break;
				}
			}
			if (toolkitsUrl == null) {
				// If we couldn't find toolkits something is wrong
				throw new RESTException("Unable to find toolkits resource from resources URL: " + resourcesUrl);
			}
		}
		return toolkitsUrl;
	}

	@Override
	public List<Toolkit> getToolkits() throws IOException {
		return Toolkit.createToolkitList(this, getToolkitsURL());
	}

	@Override
	public Toolkit getToolkit(String toolkitId) throws IOException {
		if (toolkitId.isEmpty()) {
			throw new IllegalArgumentException("Empty toolkit id");
		}
		else {
			String query = getToolkitsURL() + "/" + URLEncoder.encode(toolkitId, StandardCharsets.UTF_8.name());
			Toolkit toolkit = Toolkit.create(this, query);
			return toolkit;
		}
	}

	public Toolkit uploadToolkit(File path) throws IOException {
		return StreamsRestActions.uploadToolkit(this, path);
	}

	boolean deleteToolkit(Toolkit toolkit) throws IOException {
		return StreamsRestActions.deleteToolkit(toolkit);
	}

	private static class Resource {
		@Expose
		public String name;

		@Expose
		public String resource;
	}

	private static class ResourcesArray {
		@Expose
		public ArrayList<Resource> resources;
	}

}
