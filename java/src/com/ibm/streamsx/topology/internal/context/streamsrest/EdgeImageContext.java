/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
 */
package com.ibm.streamsx.topology.internal.context.streamsrest;

import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.JOB_CONFIG_OVERLAYS;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Arrays;
import java.util.Iterator;
import com.google.gson.JsonElement;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.build.Artifact;
import com.ibm.streamsx.rest.build.Build;
import com.ibm.streamsx.rest.build.BuildService;
import com.ibm.streamsx.rest.build.BaseImage;
import com.ibm.streamsx.rest.internal.BuildType;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
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
    }

    @Override
    public Type getType() {
        return Type.EDGE;
    }

    private Instance instance;
    private BaseImage baseImage;
    private String edgeConfigImageName = null;
    private String edgeConfigImageTag = null;
    private String edgeConfigImagePrefix = null;
    private String edgeConfigBaseImage = null;

    public Instance instance() { return instance;}

    /**
     * returns the base image to use for the image build.
     * @return
     */
    public BaseImage getBaseImage() {
        return baseImage;
    }

    private void parseEdgeConfig(JsonObject jco) {
        //System.out.println("--- parseEdgeConfig jco="+jco); 
        JsonArray jcos = GsonUtilities.array(jco, JOB_CONFIG_OVERLAYS);
        if (jcos != null) {
            Iterator<JsonElement> it = jcos.iterator();
            while (it.hasNext()) {
                JsonObject element = (JsonObject) it.next();
                if(element.has("edgeConfig")) {
                    JsonObject edgeConfig = (JsonObject) element.get("edgeConfig");
                    edgeConfigImageName = (edgeConfig.has("imageName")) ? edgeConfig.get("imageName").getAsString() : null;
                    edgeConfigImageTag = (edgeConfig.has("imageTag")) ? edgeConfig.get("imageTag").getAsString() : null;
                    edgeConfigImagePrefix = (edgeConfig.has("imagePrefix")) ? edgeConfig.get("imagePrefix").getAsString() : null;
                    edgeConfigBaseImage = (edgeConfig.has("baseImage")) ? edgeConfig.get("baseImage").getAsString() : null;
                }
            }
        }
    }

    /**
     * @see com.ibm.streamsx.topology.internal.context.streamsrest.BuildServiceContext#submitBuildArchive(com.ibm.streamsx.rest.build.BuildService, java.io.File, com.google.gson.JsonObject, com.google.gson.JsonObject, java.lang.String, com.google.gson.JsonObject)
     */
    @Override
    protected JsonObject submitBuildArchive (BuildService context, File buildArchive, JsonObject deploy, JsonObject jco,
            String buildName, JsonObject buildConfig) throws Exception {
    	parseEdgeConfig(jco);
        // find the base image to use for image build in postBuildAction()
        try {
            List<BaseImage> baseImages = context.getBaseImages();
            if (baseImages.size() == 0) {
                TRACE.severe("No base images found on build service.");
                throw new IllegalStateException("No base images found on build service.");
            }
            if (edgeConfigBaseImage != null) {
                List<String> toks = Arrays.asList(edgeConfigBaseImage.split("/"));
                if (toks.size() == 3) {
                    // edgeConfigBaseImage = registry/prefix/name:tag
                    for (BaseImage bi: baseImages) {
                        if (edgeConfigBaseImage.equals(bi.getId())) {
                            this.baseImage = bi;
                            break;
                        }
                    }
                } else {
                    // edgeConfigBaseImage expected as name:tag 
                    for (BaseImage bi: baseImages) {
                        final String biNameTag = (bi.getName() + ":" + bi.getTag());
                        if (edgeConfigBaseImage.contains(biNameTag)) {
                            this.baseImage = bi;
                            break;
                        }
                    }
                }
            }
            else {
                // no edgeConfigBaseImage given
                for (BaseImage bi: baseImages) {
                    final String biNameTagLower = (bi.getName() + " " + bi.getTag()).toLowerCase();
                    if (biNameTagLower.contains("conda") || biNameTagLower.contains("python")) {
                        this.baseImage = bi;
                        break;
                    }
                }
            }
            if (this.baseImage == null) {
            	if (null != edgeConfigBaseImage) {
            		throw new IllegalStateException("Base image '" + edgeConfigBaseImage + "' not found on build service.");
            	}
            	else {
                    this.baseImage = baseImages.get(0);
                    TRACE.warning("No base image with 'conda' or 'python' in its name or tag found. Using " + this.baseImage.getId() + " instead.");
            	}
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("No base images found on build service.", e);
        }

        return super.submitBuildArchive(context, buildArchive, deploy, jco, buildName, buildConfig);
    }

    /**
     * performs the image build
     */
    @Override
    protected void postBuildAction(JsonObject deploy, JsonObject jco, JsonObject result) throws Exception {

        if (this.baseImage == null) {
            throw new IllegalStateException("No base image available for application image build");
        }
        // create the Build service for the image build type:
        JsonObject serviceDefinition = GsonUtilities.object(deploy, StreamsKeys.SERVICE_DEFINITION);
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
        report("Building edge image");
        JsonObject buildConfigOverrides = new JsonObject();
        JsonArray applicationBundles = new JsonArray();
        JsonObject application = new JsonObject();

        JsonArray artifacts = GsonUtilities.array(GsonUtilities.object(result, "build"), "artifacts");
        JsonObject artifact0 = (JsonObject)artifacts.get(0);

        String sabUrl = artifact0.get("sabUrl").getAsString();
//        System.out.println ("---- sabUrl for buildConfigOverrides = " + sabUrl);
        application.addProperty("application", sabUrl);
        JsonObject applicationCredentials = new JsonObject();
        final String token = StreamsKeys.getBearerToken(deploy);
        applicationCredentials.addProperty("bearerToken", token);
        application.add("applicationCredentials", applicationCredentials);
        applicationBundles.add(application);
        buildConfigOverrides.add("applicationBundles", applicationBundles);
        System.out.println("INFO: baseImage = " + this.baseImage.getId());
        buildConfigOverrides.addProperty("baseImage", this.baseImage.getId());
        // use same registry and prefix as used for the base image if not set in edgeConfig
        final String imageRegistry = this.baseImage.getRegistry();
        final String imagePrefix = (edgeConfigImagePrefix != null) ? edgeConfigImagePrefix : this.baseImage.getPrefix();
        final String imageName = (edgeConfigImageName != null) ? edgeConfigImageName : getBuildName();
        final String imageTag = (edgeConfigImageTag != null) ? edgeConfigImageTag : "streamsx";
        String imageStr = imageRegistry + "/" + imagePrefix + "/" + imageName + ":" + imageTag;
        System.out.println("INFO: image = " + imageStr);
        buildConfigOverrides.addProperty("image", imageStr);

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

            TRACE.info("imageBuild ended with status " + buildStatus);
            System.out.println("INFO: imageBuild ended with status " + buildStatus);

            if (! "built".equals(buildStatus)) {
                TRACE.severe("The submitted image " + buildName + " failed to build with status " + buildStatus + ".");
                List<String> errorMessages = imageBuild.getLogMessages();
                for (String line : errorMessages) {
                    TRACE.severe(line);
                }
            }
            else {
                List<Artifact> buildArtifacts = imageBuild.getArtifacts();
//                System.out.println("imageBuilds artifacts: " + buildArtifacts);
                if (buildArtifacts == null || buildArtifacts.size() == 0)
                    throw new IllegalStateException("No image build artifacts produced.");
                if (buildArtifacts.size() != 1)
                    throw new IllegalStateException("Multiple image build artifacts produced.");
                Artifact buildArtifact = buildArtifacts.get(0);

                // add build metrics to the result JsonObject
                result.add(SubmissionResultsKeys.SUBMIT_IMAGE_METRICS, imageBuild.getMetrics());
                // total build time of bundle + image on result root level
                result.addProperty(SubmissionResultsKeys.SUBMIT_TOTAL_BUILD_TIME, 
                        buildMetrics.get(SubmissionResultsKeys.SUBMIT_TOTAL_BUILD_TIME).getAsLong() +
                        getApplicationBuild().getMetrics().get(SubmissionResultsKeys.SUBMIT_TOTAL_BUILD_TIME).getAsLong());

                artifacts = GsonUtilities.array(GsonUtilities.object(result, "build"), "artifacts");
                final String imgDigest = buildArtifact.getImageDigest();
                final String imgName = buildArtifact.getName();
                artifacts.forEach(artfct -> {
                    JsonObject jso = artfct.getAsJsonObject();
                    jso.addProperty(SubmissionResultsKeys.IMAGE_DIGEST, imgDigest);
                    jso.addProperty(SubmissionResultsKeys.DOCKER_IMAGE, imgName);
                });
                result.addProperty(SubmissionResultsKeys.IMAGE_DIGEST, imgDigest);
                result.addProperty(SubmissionResultsKeys.DOCKER_IMAGE, imgName);
            }
        }
        finally {
            if (!GsonUtilities.jboolean(deploy, KEEP_ARTIFACTS)) {
                Build applicationBuild = getApplicationBuild();
                if (applicationBuild != null) {
                    try {
                        applicationBuild.delete();
                    }
                    catch (Exception e) {
                        final String buildId = applicationBuild.getId() == null? "": applicationBuild.getId();
                        TRACE.warning("failed to delete build " + buildId + ": " + e.toString());
                    }
                }
                if (imageBuild != null) {
                    try {
                        imageBuild.delete();
                    }
                    catch (Exception e) {
                        final String buildId = imageBuild.getId() == null? "": imageBuild.getId();
                        TRACE.warning("failed to delete build " + buildId + ": " + e.toString());
                    }
                }
            }
        }
    }
}
