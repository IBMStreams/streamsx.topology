/*
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.streamsx.topology.internal.context.streamsrest;

import java.io.IOException;
import java.util.Iterator;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.internal.icp4d.ICP4DService;
import com.ibm.streamsx.rest.internal.icp4d.JobDescription;
import com.ibm.streamsx.rest.internal.icp4d.JobRunConfiguration;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.internal.context.remote.DeployKeys;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.messages.Messages;
import com.ibm.streamsx.topology.internal.streams.Util;
//import com.ibm.streamsx.topology.internal.messages.Messages;

/**
 * This context uses the Streams REST API of the Build Service to build an application,
 * and the CP4D Jobs/Spaces REST API to submit the built application. This context does not 
 * download the artifacts, i.e. the application bundle.
 * 
 * @since streamsx 1.17
 */
public class DistributedCp4dRestContext extends BuildServiceContext {

    private final static boolean CREATE_SPACE_IF_NOT_EXISTS = true;

    private JsonObject graph = null;
    private JsonObject deploy = null;

    private String projectId = null;
    private String projectName = null;
    private String spaceId = null;
    private String jobName = null;
    private boolean associateWithProject = false;
    private ICP4DService icp4dRestService;

    public DistributedCp4dRestContext() {
        super(/*downloadArtifacts=*/false);
    }

    @Override
    public Type getType() {
        return Type.DISTRIBUTED;
    }

    /**
     * Returns the Job name from the Job config if any
     * @param deploy
     * @return the job name or null if there is no job name.
     */
    private String getJobNameFromJco (JsonObject deploy) {
        //            "deploy": {
        //                "contextType": "DISTRIBUTED",
        //                "jobConfigOverlays": [
        //                  {
        //                    "jobConfig": {
        //                      "jobName": "MyTestJob"
        //                    }
        //                  }
        //                ],
        JsonObject jco = DeployKeys.copyJobConfigOverlays (deploy);
        JsonArray jcos = GsonUtilities.array(jco, DeployKeys.JOB_CONFIG_OVERLAYS);
        if (jcos == null) return null;
        Iterator<JsonElement> it = jcos.iterator();
        while (it.hasNext()) {
            JsonObject element = (JsonObject) it.next();
            if(element.has (DeployKeys.JOB_CONFIG)) {
                JsonObject jobConfig = element.get (DeployKeys.JOB_CONFIG).getAsJsonObject();
                if (jobConfig != null && jobConfig.has (DeployKeys.JOB_NAME)) {
                    return jobConfig.get (DeployKeys.JOB_NAME).getAsString();
                }
            }
        }
        return null;
    }


    /**
     * Invoked before the actual submission happens.
     * Here we extract the 'graph' JSON object for later use in {@link #postBuildAction(JsonObject, JsonObject, JsonObject)}
     * 
     * @see com.ibm.streamsx.topology.internal.context.remote.RemoteContextImpl#preSubmit(com.google.gson.JsonObject)
     */
    @Override
    protected void preSubmit (JsonObject submission) {
        super.preSubmit (submission);
        graph = GsonUtilities.jobject (submission, SUBMISSION_GRAPH);
        if (graph == null) {
            throw new IllegalStateException (SUBMISSION_GRAPH + " not found in submission");
        }
        deploy = GsonUtilities.jobject (submission, SUBMISSION_DEPLOY);
        if (deploy == null) {
            throw new IllegalStateException (SUBMISSION_DEPLOY + " not found in submission");
        }
        JsonObject service = GsonUtilities.jobject (deploy, StreamsKeys.SERVICE_DEFINITION);
        final boolean verify = sslVerify (deploy);
        icp4dRestService = ICP4DService.of (service, verify);

        if (!icp4dRestService.isExternalClient()) {
            // test the connection as the URL for REST may be quite hard coded or user provided
            try {
                icp4dRestService.test();
            } catch (IOException e) {
                System.err.println (e.getLocalizedMessage());
                final boolean userProvidedCp4dUrl = StreamsKeys.getFromServiceDefinition (deploy, StreamsKeys.CLUSTER_IP_ORIG) != null;
                final String msgKey = userProvidedCp4dUrl? "SUBMISSION_FAILED_WRONG_CP4D_URL": "SUBMISSION_FAILED_CP4D_URL_REQUIRED";
                throw new IllegalStateException (Messages.getString(msgKey));
            }
        }
        String spaceName = GsonUtilities.jstring (deploy, StreamsKeys.SPACE_NAME);
        associateWithProject = false;
        if (!icp4dRestService.isExternalClient() && spaceName == null) {
            // get "PROJECT_ID" environment variable, something like "ebb4c6be-2c2c-4e8a-8973-f470130451c8"
            // throws IllegalStateException when the environment variable is not set
            projectId = Util.getenv(Util.PROJECT_ID);
            try {
                projectName = Util.getenv(Util.PROJECT_NAME);
            } catch (IllegalStateException varNotFound) {
                // project name is only used to check if namespace == projectName when creating a job description name
                projectName = null;
            }
            associateWithProject = true;
        }
        else {
            // space name given or external client. For external client we use default space when none is given
            // need a space name and spaceId to associate the job with
            // expect a 'topology.spaceName' element in the deploy object. If not, use the Streams default space
            boolean doCreateSpace = CREATE_SPACE_IF_NOT_EXISTS;
            if (spaceName == null) {
                //use streams default space name <service_name>.<service_namespace>
                final String serviceName = StreamsKeys.getFromServiceDefinition (deploy, StreamsKeys.SERVICE_NAME);
                final String serviceNamespace = StreamsKeys.getFromServiceDefinition (deploy, StreamsKeys.SERVICE_NAMESPACE);
                if (serviceName == null || serviceNamespace == null) {
                    // can't proceed as we need the default deployment space name to get its spaceId
                    final String msg = (serviceName == null && serviceNamespace == null)?
                            "neither " + StreamsKeys.SERVICE_NAME + " nor " + StreamsKeys.SERVICE_NAMESPACE + " found in 'deploy'":
                                ((serviceName == null)? StreamsKeys.SERVICE_NAME + " not found in 'deploy'": StreamsKeys.SERVICE_NAMESPACE + " not found in 'deploy'");
                    throw new IllegalStateException (msg); 
                }
                // rule for the name of the default Streams deployment space: 
                spaceName = serviceName + "." + serviceNamespace;
                // creating the default Streams space is not our responsibility
                doCreateSpace = false;
            }

            try {
                spaceId = doCreateSpace? icp4dRestService.getOrCreateSpace(spaceName).getId(): icp4dRestService.getSpaceIdForName(spaceName);
                if (spaceId == null) {
                    final String msg = Messages.getString("DEPLOYMENT_SPACE_NOT_EXISTS", spaceName);
                    // Here we end only, when the REST call succeeded, and the query didn't find the space
                    throw new IllegalStateException (msg);
                }
            } catch (IOException e) {
                throw new IllegalStateException (e.getMessage(), e);
            }
        }
        // create a job (description) name
        jobName = getJobNameFromJco (deploy);
        if (jobName == null) {
            final String topologyNamespace = GsonUtilities.jstring (graph, "namespace");
            final String topologyName = GsonUtilities.jstring (graph, "name");
            if (associateWithProject && topologyNamespace != null && topologyNamespace.equals (projectName)) {
                // omit namespace in job description
                jobName = topologyName;
            }
            else {
                // using :: as separator looks like in Streams console, but creates a job description that cannot be edited in the CP4D GUI.
                // Obviously the validation is inconsistent
                jobName = topologyNamespace + "-" + topologyName;
            }
        }
        if (associateWithProject) {
            spaceId = null;
        } else {
            projectId = null;
        }
    }


    @Override
    protected void postBuildAction (JsonObject deploy, JsonObject jco, JsonObject result) throws Exception {
        // here we submit the Job with the SAB in the artifacts URL
        try {
            report ("Submitting job");

            JsonArray artifacts = GsonUtilities.array(GsonUtilities.object (result, "build"), "artifacts");
            // there should always be only one artifact
            JsonObject artifact0 = (JsonObject)artifacts.get(0);
            String sabUrl = artifact0.get("sabUrl").getAsString();

            JobDescription jobDescription = icp4dRestService.getOrCreateJobDescription (jobName, spaceId, projectId);
            JobRunConfiguration run = icp4dRestService.createJobRun (jobDescription, sabUrl, deploy.getAsJsonArray ("jobConfigOverlays"));
            String jobId = run.getJobId();
            report ("Job id:" + jobId);

            result.addProperty (SubmissionResultsKeys.JOB_ID, jobId);
            result.addProperty (SubmissionResultsKeys.INSTANCE_ID, run.getStreamsInstance()); 
            if (GsonUtilities.jboolean (deploy, ContextProperties.KEEP_ARTIFACTS)) {
                result.addProperty (SubmissionResultsKeys.BUNDLE_URL, run.getApplication());
            }
        }
        finally {
            if (!GsonUtilities.jboolean(deploy, ContextProperties.KEEP_ARTIFACTS)) {
                deleteBuilds (getApplicationBuild());
            }
        }
    }
}
