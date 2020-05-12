/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
 */
package com.ibm.streamsx.topology.internal.context.streamsrest;

import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.build.Build;
import com.ibm.streamsx.rest.build.BuildService;
import com.ibm.streamsx.rest.internal.BuildType;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;

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

        System.out.println("===========================================================");
        System.out.println("==== application build result: \n" + result);
        System.out.println("===========================================================");

        // create the Build service for the image build type:
        JsonObject serviceDefinition = object(deploy, StreamsKeys.SERVICE_DEFINITION);
        BuildService imageBuilder = null;
        if (serviceDefinition != null)
            imageBuilder = BuildService.ofServiceDefinition(serviceDefinition, sslVerify(deploy));
        else {
            // Remote environment context set through environment variables.
            imageBuilder = BuildService.ofEndpoint(null, null, null, null, sslVerify(deploy));
        }
        if (imageBuilder instanceof BuildServiceSetters) {
            ((BuildServiceSetters)imageBuilder).setBuildType(BuildType.STREAMS_DOCKER_IMAGE);
        }
        System.out.println("submit with build type streamsDockerImage using the imageBuilder: " + imageBuilder);
        report("Building edge image");
        JsonObject buildConfigOverrides = new JsonObject();
        JsonArray applicationBundles = new JsonArray();
        JsonObject application = new JsonObject();
        JsonArray artifacts = result.getAsJsonObject("build").getAsJsonArray("artifacts");
        JsonObject artifact0 = (JsonObject)artifacts.get(0);

        String sabUrl = artifact0.get("sabUrl").getAsString();
        System.out.println ("---- sabUrl for buildConfigOverrides = " + sabUrl);
        application.addProperty("application", sabUrl);
        JsonObject applicationCredentials = new JsonObject();
        final String token = StreamsKeys.getBearerToken(deploy);
        applicationCredentials.addProperty("bearerToken", token);
        application.add("applicationCredentials", applicationCredentials);
        applicationBundles.add(application);
        buildConfigOverrides.add("applicationBundles", applicationBundles);
        // TODO retrieve values for baseImage and image
        buildConfigOverrides.addProperty("baseImage", "image-registry.openshift-image-registry.svc:5000/edge-cpd-demo/streams-base-edge-conda-el7:v5.1_f_edge_latest");
        buildConfigOverrides.addProperty("image", "image-registry.openshift-image-registry.svc:5000/edge-cpd-demo/shalver-edge-app:shalver");
        
        Build imageBuild = null;
        try {
        	String buildName = getApplicationBuild().getName() + "_img";
            imageBuild = imageBuilder.createBuild(buildName, null);

            final long startBuildTime = System.currentTimeMillis();
            long lastCheckTime = startBuildTime;

            imageBuild.submit("buildConfigOverrides", buildConfigOverrides);

            String buildStatus;
            JsonObject buildMetrics;
            do {
                imageBuild.refresh();
                buildStatus = imageBuild.getStatus();
                buildMetrics = imageBuild.getMetrics();
                if ("built".equals(buildStatus)) {
                    final long endBuildTime = System.currentTimeMillis();
                    buildMetrics.addProperty(SubmissionResultsKeys.SUBMIT_TOTAL_BUILD_TIME, (endBuildTime - startBuildTime));
                    // build done
                    break;
                }

                String mkey = SubmissionResultsKeys.buildStateMetricKey(buildStatus);
                long now = System.currentTimeMillis();
                long duration;
                if (buildMetrics.has(mkey)) {
                    duration = buildMetrics.get(mkey).getAsLong();
                } else {
                    duration = 0;
                }
                duration += (now - lastCheckTime);
                buildMetrics.addProperty(mkey, duration);
                lastCheckTime = now;

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw e;
                }
                buildStatus = imageBuild.getStatus();
            } while ("building".equals(buildStatus) || "waiting".equals(buildStatus) || "submitted".equals(buildStatus));
            
            System.out.println("imageBuild ended with status " + buildStatus);
            System.out.println("imageBuilds artifacts: " + imageBuild.getArtifacts());
            
            if (! "built".equals(buildStatus)) {
                TRACE.severe("The image failed to build with status " + buildStatus + ".");
                throw new IllegalStateException("Error submitting bundle for build edge image: " + buildName);
            }
            else {
            	// add build metrics to the result JsonObject
            	result.add(SubmissionResultsKeys.SUBMIT_IMAGE_METRICS, imageBuild.getMetrics());
                // TODO add "something" from the artifact to the result JsonObject ?
            }

        }
        finally {
            if (!jboolean(deploy, KEEP_ARTIFACTS)) {
                Build applicationBuild = getApplicationBuild();
                if (applicationBuild != null) {
                    try {
                        applicationBuild.delete();
                        System.out.println ("application build deleted.");
                    }
                    catch (Exception e) {
                        TRACE.warning(e.toString());
                    }
                }
                if (imageBuild != null) {
                    try {
                        imageBuild.delete();
                        System.out.println ("imageBuild deleted");
                    }
                    catch (Exception e) {
                        System.out.println (e);
                    }

                }
            }
        }
    }
}
