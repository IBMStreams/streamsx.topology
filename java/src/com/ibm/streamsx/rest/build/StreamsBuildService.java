package com.ibm.streamsx.rest.build;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;
import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.internal.BuildType;
import com.ibm.streamsx.rest.internal.RestUtils;
import com.ibm.streamsx.rest.internal.StandaloneAuthenticator;
import com.ibm.streamsx.topology.internal.context.streamsrest.BuildServiceSetters;
import com.ibm.streamsx.topology.internal.streams.Util;

class StreamsBuildService extends AbstractConnection implements BuildService, BuildServiceSetters {

    static final String STREAMS_REST_RESOURCES = "/streams/rest/resources";
    static final String STREAMS_BUILD_PATH = "/streams/rest/builds";

    static BuildService of(Function<Executor,String> authenticator, JsonObject serviceDefinition,
            boolean verify) throws IOException {

        String buildServiceEndpoint = jstring(object(serviceDefinition, "connection_info"), "serviceBuildEndpoint");
        String buildServicePoolsEndpoint = jstring(object(serviceDefinition, "connection_info"), "serviceBuildPoolsEndpoint");
        // buildServicePoolsEndpoint is null when "connection_info" JSON element has no "serviceBuildPoolsEndpoint"
        if (authenticator instanceof StandaloneAuthenticator) {
            if (buildServiceEndpoint == null) {
                buildServiceEndpoint = Util.getenv(Util.STREAMS_BUILD_URL);
            }
            if (!buildServiceEndpoint.endsWith(STREAMS_BUILD_PATH)) {
                // URL was user-provided root of service, add the path
                URL url = new URL(buildServiceEndpoint);
                URL buildsUrl = new URL(url.getProtocol(), url.getHost(), url.getPort(), STREAMS_BUILD_PATH);
                buildServiceEndpoint = buildsUrl.toExternalForm();
            }
            return StreamsBuildService.of(authenticator, buildServiceEndpoint, verify);
        }
        return new StreamsBuildService(buildServiceEndpoint, buildServicePoolsEndpoint, authenticator, verify);
    }

    static BuildService of(Function<Executor,String> authenticator, String buildServiceEndpoint,
            boolean verify) throws IOException {

        if (buildServiceEndpoint == null) {
            buildServiceEndpoint = Util.getenv(Util.STREAMS_BUILD_URL);
            if (!buildServiceEndpoint.endsWith(STREAMS_BUILD_PATH)) {
                // URL was user-provided root of service, add the path
                URL url = new URL(buildServiceEndpoint);
                URL buildsUrl = new URL(url.getProtocol(), url.getHost(), url.getPort(), STREAMS_BUILD_PATH);
                buildServiceEndpoint = buildsUrl.toExternalForm();
            }
        }
        return new StreamsBuildService(buildServiceEndpoint, null, authenticator, verify);
    }
	
	private static final String TOOLKITS_RESOURCE_NAME = "toolkits";

	private String endpoint;
	private String poolsEndpoint;
	private BuildType buildType = BuildType.APPLICATION;
	private String toolkitsUrl;
	private Function<Executor, String> authenticator;

	private StreamsBuildService(String endpoint, String poolsEndpoint, Function<Executor, String> authenticator, boolean verify) throws MalformedURLException {
		super(!verify);
		this.endpoint = endpoint;
		this.poolsEndpoint = poolsEndpoint;
		this.authenticator = authenticator;
	}

    @Override
    public void setBuildType(BuildType buildType) {
        this.buildType = buildType;
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
	public Build createBuild(String name, JsonObject buildConfig) throws IOException {
		
		JsonObject buildParams = new JsonObject();

		buildParams.addProperty("type", buildType.getJsonValue());
		buildParams.addProperty("incremental", false);		
		if (name != null)
			buildParams.addProperty("name", name);
		String bodyStr = buildParams.toString();
//        System.out.println("StreamsBuildService: =======> POST body = " + bodyStr);
		Request post = Request.Post(endpoint)	      
		    .addHeader("Authorization", getAuthorization())
		    .bodyString(bodyStr,
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

	/**
     * @see com.ibm.streamsx.rest.build.BuildService#getBaseImages()
     */
    @Override
    public List<BaseImage> getBaseImages() throws IOException {
        final String BUILD_POOLS = "buildPools";
        if (this.poolsEndpoint == null) {
            // exposed endpoint for the build pools is optional
            return Collections.emptyList();
        }
        // find out the right build pool; we use the first build pool with type 'image'
        JsonObject jsonResponse = new Gson().fromJson(this.getResponseString(this.poolsEndpoint), JsonObject.class);
        if (!jsonResponse.has(BUILD_POOLS)) {
            throw new IOException("No 'buildPools' JSON element at " + this.poolsEndpoint + ".");
        }
        if (!jsonResponse.get(BUILD_POOLS).isJsonArray()) {
            throw new IOException("The '" + BUILD_POOLS + "' JSON element is not an Array.");
        }
        JsonArray pools = jsonResponse.getAsJsonArray(BUILD_POOLS);
        String poolId = null;
        for (JsonElement pool: pools) {
            final String poolType = pool.getAsJsonObject().get("type").getAsString();
            if ("image".equals(poolType)) {
                poolId = pool.getAsJsonObject().get("restid").getAsString();
                break;
            }
        }
        if (poolId == null) {
            throw new IOException("No build pool of 'image' type found.");
        }
        final String baseImagesUri = this.poolsEndpoint + (this.poolsEndpoint.endsWith("/")? "": "/") + poolId + "/baseimages";
        return BaseImage.createImageList(this, baseImagesUri);
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
