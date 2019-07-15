package com.ibm.streamsx.rest.build;

import java.io.IOException;
import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.internal.RestUtils;

public class StreamsBuildService extends AbstractConnection implements BuildService {
	
	private static final String TOOLKITS_RESOURCE_NAME = "toolkits";

	private String endpoint;
	private String authorization;
	private String toolkitsUrl;

	public StreamsBuildService(String endpoint, String bearerToken) {
		super(false);
		this.endpoint = endpoint;
		setAuthorization(bearerToken);
	}
	
	private void setAuthorization(String bearerToken) {
		authorization = RestUtils.createBearerAuth(bearerToken);
	}
	
	@Override
	String getAuthorization() {
		return authorization;
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
		// Query the resourcesUrl to find the instances URL
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

    Toolkit putToolkit(File path) throws IOException {
	return StreamsRestActions.putToolkit(this, path);
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
