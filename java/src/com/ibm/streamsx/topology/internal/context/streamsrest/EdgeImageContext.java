/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
 */
package com.ibm.streamsx.topology.internal.context.streamsrest;

import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.net.URL;
import java.util.function.Function;

import org.apache.http.client.fluent.Executor;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.rest.build.BuildService;
import com.ibm.streamsx.rest.build.BuildService.BuildType;
import com.ibm.streamsx.rest.internal.ICP4DAuthenticator;
import com.ibm.streamsx.rest.internal.StandaloneAuthenticator;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * Streams V5 (ICP4D) build service context for EDGE.
 * This context submits a build with "streamsDockerImage" type. It creates an image (for example docker)
 * by using a previously creates application build, which is kept on the build service.
 * 
 */
public class EdgeImageContext extends BuildServiceContext {

    /**
     * @param downloadArtifacts
     */
    public EdgeImageContext() {
        super(/*downloadArtifacts=*/false);
        System.out.println (this.getClass().getName() + " instantiated");
    }

    @Override
    public Type getType() {
        return Type.EDGE;
    }

    private Instance instance;

    public Instance instance() { return instance;}


    /**
     * performs the image build
     */
    @Override
    protected void postBuildAction(JsonObject deploy, JsonObject jco, JsonObject result) throws Exception {
        // create the Build service for the image build type:
        JsonObject serviceDefinition = object(deploy, StreamsKeys.SERVICE_DEFINITION);
        BuildService imageBuilder = null;
        if (serviceDefinition != null)
            imageBuilder = BuildService.ofServiceDefinition(serviceDefinition, sslVerify(deploy), BuildType.STREAMS_DOCKER_IMAGE);
        else {
            // Remote environment context set through environment variables.
            imageBuilder = BuildService.ofEndpoint(null, null, null, null, sslVerify(deploy), BuildType.STREAMS_DOCKER_IMAGE);
        }

        System.out.println("BuildServiceEdgeContext.postBuildAction: result = " + result);
        System.out.println("BuildServiceEdgeContext.postBuildAction: jco = " + jco);
        System.out.println("TODO: submit with build type streamsDockerImage using the imageBuilder: " + imageBuilder);

        if (instance == null) {

            URL instanceUrl = new URL(StreamsKeys.getStreamsInstanceURL(deploy));

            String path = instanceUrl.getPath();
            URL restUrl;
            if (path.startsWith("/streams/rest/instances/")) {
                restUrl = new URL(instanceUrl.getProtocol(),
                        instanceUrl.getHost(), instanceUrl.getPort(),
                        "/streams/rest/resources");
            } else {
                restUrl = new URL(instanceUrl.getProtocol(),
                        instanceUrl.getHost(), instanceUrl.getPort(),
                        path.replaceFirst("/streams-rest/", "/streams-resource/"));
            }

            String name = jstring(serviceDefinition, "service_name");
            Function<Executor, String> authenticator = (name == null
                    || name.isEmpty())
                    ? StandaloneAuthenticator.of(serviceDefinition)
                            : ICP4DAuthenticator.of(serviceDefinition);
                    StreamsConnection conn = StreamsConnection
                            .ofAuthenticator(restUrl.toExternalForm(), authenticator);

                    if (!sslVerify(deploy))
                        conn.allowInsecureHosts(true);

                    // Create the instance directly from the URL
                    instance = conn.getInstance(instanceUrl.toExternalForm());
        }

        JsonArray artifacts = GsonUtilities.array(GsonUtilities.object(result, "build"), "artifacts");

        try {
            if (artifacts == null || artifacts.size() == 0)
                throw new IllegalStateException("No build artifacts produced.");
            if (artifacts.size() != 1)
                throw new IllegalStateException("Multiple build artifacts produced.");




        } finally {
            if (!jboolean(deploy, KEEP_ARTIFACTS)) {
                //TODO: delete build on build service?
            }
        }
    }

}
