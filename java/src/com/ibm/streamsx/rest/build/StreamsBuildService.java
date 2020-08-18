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
import java.util.List;
import java.util.function.Function;

import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import com.google.gson.GsonBuilder;
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
            // TODO: URL completion cannot be done as the build path depends on Streams version - commented out
//            if (!buildServiceEndpoint.endsWith(STREAMS_BUILD_PATH)) {
//                // URL was user-provided root of service, add the path
//                URL url = new URL(buildServiceEndpoint);
//                URL buildsUrl = new URL(url.getProtocol(), url.getHost(), url.getPort(), STREAMS_BUILD_PATH);
//                buildServiceEndpoint = buildsUrl.toExternalForm();
//            }
            return StreamsBuildService.of(authenticator, buildServiceEndpoint, verify);
        }
        return new StreamsBuildService(buildServiceEndpoint, buildServicePoolsEndpoint, authenticator, verify);
    }

    static BuildService of(Function<Executor,String> authenticator, String buildServiceEndpoint,
            boolean verify) throws IOException {

        if (buildServiceEndpoint == null) {
            buildServiceEndpoint = Util.getenv(Util.STREAMS_BUILD_URL);
            // TODO: URL completion cannot be done as the build path depends on Streams version - commented out
//            if (!buildServiceEndpoint.endsWith(STREAMS_BUILD_PATH)) {
//                // URL was user-provided root of service, add the path
//                URL url = new URL(buildServiceEndpoint);
//                URL buildsUrl = new URL(url.getProtocol(), url.getHost(), url.getPort(), STREAMS_BUILD_PATH);
//                buildServiceEndpoint = buildsUrl.toExternalForm();
//            }
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
		if (name != null) {
			buildParams.addProperty("name", name);
		}
		String bodyStr = buildParams.toString();
		Request post = Request.Post(endpoint)	      
		    .addHeader("Authorization", getAuthorization())
		    .bodyString(bodyStr,
		                ContentType.APPLICATION_JSON);
		
		Build build = Build.create(this, this, StreamsRestUtils.requestGsonResponse(executor, post));
		return build;
	}

	String getToolkitsURL() throws IOException {
		if (toolkitsUrl == null) {
		    // Examples for external build endpoints:
		    // CPD < 3.5:  https://ivan34-cpd-ivan34.apps.cpstreamsx6.cp.fyre.ibm.com/streams-build/instances/sample-streams/ivan34/
		    // CPD >= 3.5: https://nbgf2-cpd-nbgf2.apps.cpstreamsx8.cp.fyre.ibm.com/streams_build_service/v1/namespaces/nbgf2/instances/sample-streams/builds/

		    // Examples for internal endpoints:
		    // CPD < 3.5:  serviceBuildEndpoint": "https://build-sample-streams-build.ivan34:8445/streams/rest/builds"
		    // CPD >= 3.5: serviceBuildEndpoint": "https://build-sample-streams-build.nbgf2:8445/streams/v1/builds"
		    URL epUrl = new URL(endpoint);
			String urlPath;
			if (epUrl.getPath().startsWith("/streams-build/")) {
			    // external URL, CPD < 3.5
			    // /streams-build/instances/sample-streams/ivan34/
			    // /streams-build-resource/instances/sample-streams/ivan34/
			    urlPath = epUrl.getPath().replaceFirst("^/streams-build/", "/streams-build-resource/");
			} else if (epUrl.getPath().startsWith("/streams_build_service/v1/")) {
			    // external URL, CPD >= 3.5
			    // /streams_build_service/v1/namespaces/nbgf2/instances/sample-streams/builds/
			    // /streams_build_service/v1/namespaces/nbgf2/instances/sample-streams/roots/
			    urlPath = epUrl.getPath().replaceFirst("/builds[/]?$", "/roots");
			} else if (epUrl.getPath().startsWith("/streams/rest/")) {
			    // internal URL, CPD < 3.5
			    // https://build-sample-streams-build.ivan34:8445/streams/rest/builds
			    // https://build-sample-streams-build.ivan34:8445/streams/rest/resources
			    urlPath = epUrl.getPath().replaceFirst("/builds[/]?$", "/resources");
			} else if (epUrl.getPath().startsWith("/streams/v1/")) {
			    // internal URL, CPD >= 3.5
			    // https://build-sample-streams-build.nbgf2:8445/streams/v1/builds
			    // https://build-sample-streams-build.nbgf2:8445/streams/v1/roots
			    urlPath = epUrl.getPath().replaceFirst("/builds[/]?$", "/roots");
			}
			else {
			    // use the path "as-is"
			    urlPath = epUrl.getPath();
			}
			String resourcesUrl = (new URL(epUrl.getProtocol(),
			        epUrl.getHost(),
			        epUrl.getPort(),
			        urlPath)).toExternalForm();
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
			    // try to find toolkits URL under the application build pool
		        if (this.poolsEndpoint == null) {
		         // If we couldn't find toolkits something is wrong
	                throw new RESTException("Unable to find toolkits resource in application build pool: No buildPools endpoint determined.");
		        }
		        // find out the right build pool; we use the first build pool with type 'image'
		        final String poolType = "application";
		        List<BuildPool> appBuildPools = BuildPool.createPoolList(this, this.poolsEndpoint, poolType);
		        if (appBuildPools.size() == 0) {
		            throw new RESTException("No build pool of '" + poolType + "' type found.");
		        }
		        toolkitsUrl = appBuildPools.get(0).getToolkits();
			}
			if (toolkitsUrl == null) {
				// If we couldn't find toolkits something is wrong
				throw new RESTException("Unable to find toolkits resource from resources URL: " + resourcesUrl + " or application build pool");
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
        if (this.poolsEndpoint == null) {
            // exposed endpoint for the build pools is optional, but required for getting base images
            throw new IOException("No REST build pool endpoint available.");
        }
        // find out the right build pool; we use the first build pool with type 'image'
        final String poolType = "image";
        List<BuildPool> imageBuildPools = BuildPool.createPoolList(this, this.poolsEndpoint, poolType);
        if (imageBuildPools.size() == 0) {
            throw new IOException("No build pool of '" + poolType + "' type found.");
        }
        final String poolId = imageBuildPools.get(0).getRestid();
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
