/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Access to a Streaming Analytics service on IBM Cloud.
 * 
 * @since 1.8
 */
public interface StreamingAnalyticsService {
    
    /**
     * Access to a Streaming Analytics service on IBM Cloud.
     * 
     * <BR>
     *  When specified {@code vcapServices} may be one of:
     * <UL>
     * <LI>An object representing VCAP service definitions.</LI>
     * <LI>A string representing a file containing VCAP service definitions.</LI>
     * </UL>
     * If {@code vcapServices} is {@code null} then the environment
     * variable {@code VCAP_SERVICES} must contain the valid service
     * definitions and credentials.
     * <BR>
     * If {@code serviceName} is {@code null} then the environment
     * variable {@code STREAMING_ANALYTICS_SERVICE_NAME} must contain the name of
     * the required service.
     * <BR>
     * The service named by {@code serviceName} must exist in the
     * defined VCAP services.
     *
     * @param vcapServices
     *            JSON representation of VCAP service definitions.
     * @param serviceName
     *            Name of the Streaming Analytics service to access.
     *            
     * @return {@code StreamingAnalyticsService} for {@code serviceName}.
     * @throws IOException
     */
    static StreamingAnalyticsService of(JsonElement vcapServices,
            String serviceName) throws IOException {

        JsonObject config = new JsonObject();

        if (serviceName != null)
            config.addProperty(SERVICE_NAME, serviceName);
        if (vcapServices != null)
            config.add(VCAP_SERVICES, vcapServices);
        
        System.err.println("SAS:" + config);


        return AbstractStreamingAnalyticsService.of(config);
    }
    
    /**
     * Submit a Streams bundle to run on the Streaming Analytics Service.
     * <p>The JSON object may contain an optional {@code deploy} member that
     * includes deployment information.
     * @param bundle A streams application bundle
     * @param submission Deployment info to be submitted.
     * @return The job id, or -1. Results from the submit will be added to the
     * submission parameter object as a member named @{code submissionResults}.
     * @throws IOException
     */
    BigInteger submitJob(File bundle, JsonObject submission) throws IOException;

    /**
     * Submit an archive to build on the Streaming Analytics Service, and submit
     * the job if the build is successful.
     * <p>
     * The JSON object contains two keys:
     * <UL>
     * <LI>{@code deploy} - Optional - Deployment information.</LI>
     * <LI>{@code graph} - Required - JSON representation of the topology graph.</LI>
     * </UL>
     * <p>
     * Results are added to the submission parameters object.
     * @param archive The application archive to build.
     * @param submission Topology and deployment info to be submitted.
     * @return The job id, or -1. Results from the submit will be added to the
     * submission parameter object as a member named @{code submissionResults}.
     * @throws IOException
     */
    BigInteger buildAndSubmitJob(File archive, JsonObject submission) throws IOException;

    /**
     * Gets the {@link Instance IBM Streams Instance} object for the Streaming
     * Analytics service.
     * @return an {@link Instance IBM Streams Instance} associated with this
     * service.
     * @throws IOException
     */
    Instance getInstance() throws IOException;
}
