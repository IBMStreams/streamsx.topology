/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;

import java.io.File;
import java.io.IOException;

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
     * <LI>A string representing the serialized JSON VCAP service definitions.</LI>
     * <LI>A string representing a file containing VCAP service definitions.</LI>
     * </UL>
     * If {@code vcapServices} is {@code null} then the environment
     * variable {@code VCAP_SERVICES} must either contain the valid service
     * definitions and credentials or point to a file containing the definitions
     * and credentials.
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
        
        return AbstractStreamingAnalyticsService.of(config);
    }
    
    /**
     * Submit a Streams bundle to run on the Streaming Analytics Service.
     * @param bundle A streams application bundle
     * @param jco Job configuration overlay in JSON format.
     * @return The Job, or null.
     * @throws IOException
     */
    Job submitJob(File bundle, JsonObject jco) throws IOException;

    /**
     * Submit an archive to build on the Streaming Analytics Service, and submit
     * the job if the build is successful.
     * @param archive The application archive to build.
     * @param jco Job configuration overlay in JSON format.
     * @param buildName A name for the build, or null.
     * @return The Job, or null.
     * @throws IOException
     */
    Job buildAndSubmitJob(File archive, JsonObject jco, String buildName) throws IOException;

    /**
     * Gets the {@link Instance IBM Streams Instance} object for the Streaming
     * Analytics service.
     * @return an {@link Instance IBM Streams Instance} associated with this
     * service.
     * @throws IOException
     */
    Instance getInstance() throws IOException;
}
