/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
 */
package com.ibm.streamsx.topology.internal.context.streamsrest;

import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.IOException;

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
        System.out.println("TODO: submit with build type streamsDockerImage using the imageBuilder: " + imageBuilder);
        report("Building edge image");
        JsonObject buildConfig = new JsonObject();
        JsonArray applicationBundles = new JsonArray();
        JsonObject application = new JsonObject();
        JsonArray artifacts = result.getAsJsonObject("build").getAsJsonArray("artifacts");
        JsonObject artifact0 = (JsonObject)artifacts.get(0);

        String sabUrl = artifact0.get("sabUrl").getAsString();
        System.out.println ("---- sabUrl for Buildconfig = " + sabUrl);
        application.addProperty("application", sabUrl);
        applicationBundles.add(application);
        buildConfig.add("applicationBundles", applicationBundles);
        
        //        
        //        "applicationBundles": [
        //                               { "application": "https://10.6.24.71:31233/streams/rest/builds/2/artifacts/0/applicationbundle",
        //                                 "applicationCredentials": {
        //                                   "user": "streamsadmin",
        //                                   "password": "install"
        //                                 }
        //                               }
        //                             ],
        //
        //        {
        //            "submitMetrics": {
        //               "buildArchiveSize":3728424,
        //               "buildArchiveUploadTime_ms":5455,
        //               "buildState_submittedTime_ms":723,
        //               "buildState_buildingTime_ms":30759,
        //               "totalBuildTime_ms":32705},
        //            "build": {
        //               "name":null,
        //               "artifacts":[
        //                            {
        //                                "sabUrl":"https://edge-cpd-demo-cpd-edge-cpd-demo.apps.streams-ocp-43-1.os.fyre.ibm.com:443/streams-build/instances/sample-streams/edge-cpd-demo/82/artifacts/0/applicationbundle"
        //                            }
        //                           ]
        //             }
        //         }


        // TODO: create a valid build config
        buildConfig.addProperty("baseImage", "image-registry.openshift-image-registry.svc:5000/edge-cpd-demo/streams-base-edge-conda-el7:v5.1_f_edge_latest");
        buildConfig.addProperty("image", "image-registry.openshift-image-registry.svc:5000/edge-cpd-demo/shalver-edge-app:shalver");
        buildConfig.addProperty("baseImageName", "streams-base-edge-conda-el7");
/*
        buildConfig.addProperty("baseImageRegistry", "image-registry.openshift-image-registry.svc:5000");
        buildConfig.addProperty("baseImagePrefix", "<namespace>");
        buildConfig.addProperty("baseImageName", "streams-base-edge-conda-el7");
        buildConfig.addProperty("baseImageTag", "v5.1_f_edge_latest");
        buildConfig.addProperty("imageRegistry", "image-registry.openshift-image-registry.svc:5000");
        buildConfig.addProperty("imagePrefix", "<namespace>");
        buildConfig.addProperty("imageName", "shalver-edge-app");
        buildConfig.addProperty("imageTag", "shalver");
 */
        System.out.println ("Buildconfig = " + buildConfig);
        
        Build imageBuild = null;
        try {
            imageBuild = imageBuilder.createBuild(getApplicationBuild().getName() + "_img", buildConfig);

            final long startBuildTime = System.currentTimeMillis();
            long lastCheckTime = startBuildTime;

            imageBuild.submit();

            String buildStatus;
            do {
                imageBuild.refresh();
                buildStatus = imageBuild.getStatus();
                JsonObject buildMetrics = imageBuild.getMetrics();
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
                // TODO: Make error handling regarding the result when postBuildAction(...) in DistributedStreamsRestContext fails?
            }
            else {
                // TODO add "something" to the result JsonObject
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
