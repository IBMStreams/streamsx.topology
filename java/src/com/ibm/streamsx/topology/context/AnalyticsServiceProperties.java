package com.ibm.streamsx.topology.context;

public interface AnalyticsServiceProperties {
    /**
     * Name of the service to use as a {@code String}.
     * The information for the service will be extracted
     * from the VCAP using this name. This property must be
     * set when submitting to {@link StreamsContext.Type#ANALYTICS_SERVICE}.
     */
    String SERVICE_NAME = "topology.service.name";
    
    /**
     * Definition of IBM Bluemix services. The value
     * may be:
     * <UL>
     * <LI>{@code java.io.File} - File containing the VCAP services JSON.</LI>
     * <LI>{@code String} - String containing the VCAP services serialized JSON.</LI>
     * <LI>{@code com.ibm.json.java.JSONObject} - JSON object containing the VCAP services.</LI>
     * </UL>
     */
    String VCAP_SERVICES = "topology.service.vcap";
}
