package com.ibm.streamsx.rest.internal.icp4d;

import java.io.IOException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Represents the REST service of the ICP4D platform.
 * Abstracts the REST API of the CP4D.
 *
 * Use this interface only with CP4D >= 3.5 and Streams >= 5.5.
 * @since 1.17
 */
public interface ICP4DService {

    /**
     * Creates a new ICP4DService instance from the service definition JSON object.
     * CP4D must be at least version 3.5.
     * @param service the service definition object 'topology.service.definition'
     * @return a new Service instance
     */
    static ICP4DService of (JsonObject service, final boolean verify) {
        //        JsonObject service = GsonUtilities.jobject (deploy, StreamsKeys.SERVICE_DEFINITION);
        final ICP4DUserAuthenticator authenticator = ICP4DUserAuthenticator.of (service, verify);
        return new ICP4DServiceImpl (service, verify, authenticator);
    }

    /**
     * Returns the REST URl for the jobs REST API, for example https://host.com/v2/jobs
     * @return the URL for the Jobs API
     */
    String getJobsRestUrl();

    /**
     * Returns the REST URl for the spaces REST API, for example https://host.com/v2/spaces
     * @return the URL for the Jobs API
     */
    String getSpacesRestUrl();

    /**
     * true if the client is external to the CP4D, false otherwise.
     * @return
     */
    boolean isExternalClient();

    /**
     * Performs a GET request on the Spaces API to verify it is working
     * @throws IOException test failed
     */
    void test() throws IOException;

    /**
     * Gets the spaceId for a deployment space given by its name.
     * @param spaceName the space name
     * @return the space ID or null if the space name doesn't exist.
     * @throws IOException 
     */
    String getSpaceIdForName (String spaceName) throws IOException;

    /**
     * Gets or creates a CP4D deployment space.
     * @param spaceName the name of the deployment space
     * @return the Space object.
     */
    DeploymentSpace getOrCreateSpace (String spaceName) throws IOException;

    /**
     * Gets or creates a CP4D job description associated with either a project or a deployment space.
     * @param jobName the name of the job
     * @param spaceId the space_id to associate the job with a deployment space. <tt>projectId</tt> must be null.
     * @param projectId the project_id to associate the job with a project. <tt>space_id</tt> must be null.
     * @return the Job object.
     */
    JobDescription getOrCreateJobDescription (String jobName, String spaceId, String projectId) throws IOException;

    /**
     * Creates a Job run for a job description
     * @param jobDescrition the job description
     * @param sabUrl the URL of the Streams application bundle
     * @param jobConfigOverlaysArray the job configurations
     * @return a JobRunConfiguration instance
     * @throws IOException
     */
    JobRunConfiguration createJobRun (JobDescription jobDescrition, String sabUrl, JsonArray jobConfigOverlaysArray) throws IOException;
}
