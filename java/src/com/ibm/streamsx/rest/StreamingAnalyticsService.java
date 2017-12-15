/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_DEFINITION;
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
     * Access to a Streaming Analytics service from service name and VCAP services.
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
     * @throws IOException Error connecting to the service.
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
     * Access to a Streaming Analytics service from a service definition.
     * 
     * A service definition is a JSON object describing a Streaming Analytics service and may be one of:
     * <UL>
     * <LI>The service credentials copied from the <em>Service credentials</em> page of the service console
     * (not the Streams console). Credentials are provided in JSON format. The JSON snippet lists credentials,
     * such as the API key and secret, as well as connection information for the service.</LI>
     * <LI>A JSON object of the form:
     * <code>{ "type": "streaming-analytics", "name": "</code><em>service-name</em><code>": "credentials": { ... } } </code>
     * with the service credentials as the value of the {@code credentials} key.
     * </LI>
     * </UL>
     * 
     * @param serviceDefinition Definition of the service to access.
     * @throws IOException Error connecting to the service.
     * @since 1.8
     */
    static StreamingAnalyticsService of(JsonObject serviceDefinition) throws IOException {
                
        JsonObject config = new JsonObject();
        config.add(SERVICE_DEFINITION, serviceDefinition);
        
        return AbstractStreamingAnalyticsService.of(config);
    }
    
    /**
     * Get the service name.
     * If the name is unknown then {@code service} is returned.
     * @return Service name or {@code service} if it is unknown.
     */
    String getName();
    
    /**
     * Check the status of this Streaming Analytics Service.
     * <P>
     * The returned {@link Result} instance has:
     * <UL>
     * <LI>{@link Result#getId()} returning {@code null}.</LI>
     * <LI>{@link Result#getElement()} returning {@code this}.</LI>
     * <LI>{@link Result#getRawResult()} return the raw JSON response.</LI>
     * <LI>{@link Result#isOk()} returns {@code true} if the service is running otherwise {@code false}.</LI>
     * </UL>
     * </P>
     * 
     * @param requireRunning If {@code true} and the service is not running then an {@code IllegalStateException}
     * is thrown, otherwise the return indicates the status of the service.
     * 
     * @return Result of the status check.
     * 
     * @throws IOException Error communicating with the service.
     * @throws IllegalStateException {@code requireRunning} is {@code true} and the service is not running.
     */
    Result<StreamingAnalyticsService,JsonObject> checkStatus(boolean requireRunning) throws IOException;
    
    /**
     * Submit a Streams bundle to run on the Streaming Analytics Service.
     * <P>
     * The returned {@link Result} instance has:
     * <UL>
     * <LI>{@link Result#getId()} returning the job identifier or {@code null} if
     * a job was not created..</LI>
     * <LI>{@link Result#getElement()} returning a {@link Job} instance for the submitted job or {@code null} if
     * a job was not created.</LI>
     * <LI>{@link Result#getRawResult()} return the raw JSON response.</LI>
     * </UL>
     * </P>
     * @param bundle A streams application bundle
     * @param jco Job configuration overlay in JSON format.
     * @return Result of the job submission.
     * @throws IOException Error communicating with the service.
     */
    Result<Job,JsonObject> submitJob(File bundle, JsonObject jco) throws IOException;

    /**
     * Submit an archive to build on the Streaming Analytics Service, and submit
     * the job if the build is successful.
     * <P>
     * The returned {@link Result} instance has:
     * <UL>
     * <LI>{@link Result#getId()} returning the job identifier or {@code null} if
     * a job was not created..</LI>
     * <LI>{@link Result#getElement()} returning a {@link Job} instance for the submitted job or {@code null} if
     * a job was not created.</LI>
     * <LI>{@link Result#getRawResult()} return the raw JSON response.</LI>
     * </UL>
     * </P>
     * @param archive The application archive to build.
     * @param jco Job configuration overlay in JSON format.
     * @param buildName A name for the build, or null.
     * @return Result of the build and job submission.
     * @throws IOException Error communicating with the service.
     */
    Result<Job,JsonObject> buildAndSubmitJob(File archive, JsonObject jco, String buildName) throws IOException;

    /**
     * Gets the {@link Instance IBM Streams Instance} object for the Streaming
     * Analytics service.
     * @return an {@link Instance IBM Streams Instance} associated with this
     * service.
     * @throws IOException
     */
    Instance getInstance() throws IOException;
}
