/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.context;

/**
 * Configuration properties for the Streaming Analytics service
 * on IBM Cloud.
 * 
 * When submitting to a Streaming Analytics service <B>one</B>
 * of these two sets of information must be provided:
 * <UL>
 * <LI>
 * {@linkplain #SERVICE_DEFINITION Service definition}.
 * </LI>
 * <LI>
 * IBM Cloud {@linkplain #VCAP_SERVICES service definitions} and
 * a {@linkplain #SERVICE_NAME service name}. The named service must exist in
 * service definitions and must be of type {@code streaming-analytics}.
 * </LI>
 * </UL>
 * A {@code SERVICE_DEFINITION} takes precedence over {@code VCAP_SERVICES} and
 * {@code SERVICE_NAME}.
 *
 * @see StreamsContext.Type#STREAMING_ANALYTICS_SERVICE
*/
public interface AnalyticsServiceProperties {
    /**
     * Name of the service to use as a {@code String}.
     * The information for the service will be extracted
     * from the VCAP service definitions using this name. This property must be
     * set when submitting to {@link StreamsContext.Type#STREAMING_ANALYTICS_SERVICE}.
     */
    String SERVICE_NAME = "topology.service.name";
    
    /**
     * IBM Cloud service definitions.
     * 
     * The value may be:
     * <UL>
     * <LI>{@code java.io.File} - File containing the VCAP services JSON.</LI>
     * <LI>{@code String} - String containing the VCAP services serialized JSON.</LI>
     * <LI>{@code com.ibm.json.java.JSONObject} - JSON object containing the VCAP services.</LI>
     * </UL>
     * If not set then the environment variable {@code VCAP_SERVICES} is assumed
     * to contain service definitions directly or a file name containing the definitions.
     */
    String VCAP_SERVICES = "topology.service.vcap";
    
    /**
     * Definition of a Streaming Analytics service. A service definition is a JSON
     * object describing a Streaming Analytics service.
     * <UL>
     * <LI>The <em>service credentials</em> copied from the <em>Service credentials</em>
     * page of the service console (not the Streams console).
     * Credentials are provided in JSON format. They contain such as
     * the API key and secret, as well as connection information for the service.
     * <LI>A JSON object of the form:
     * <code>{ "type": "streaming-analytics", "name": "</code><em>service-name</em><code>", "credentials": {...} }</code>
     * with the <em>service credentials</em> as the value of the {@code credentials} key</LI>
     * </UL>
     * 
     * @since 1.8
     */
    String SERVICE_DEFINITION = "topology.service.definition";
}
