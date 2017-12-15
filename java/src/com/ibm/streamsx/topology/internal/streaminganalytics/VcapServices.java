/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017  
 */
package com.ibm.streamsx.topology.internal.streaminganalytics;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_DEFINITION;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * Utilities to get the correct VCAP services information for submission to
 * Streaming Analytics Service.
 *
 */
public class VcapServices {

    /**
     * Get the top-level VCAP services object.
     * 
     * Object can be one of the following:
     * <ul>
     * <li>JsonObject - assumed to contain VCAP_SERVICES </li>
     * <li>String - assumed to contain serialized VCAP_SERVICES JSON, or the
     * location of a file containing the serialized VCAP_SERVICES JSON</li>
     * <li>null - assumed to be in the environment variable VCAP_SERVICES</li>
     * </ul>
     */
    public static JsonObject getVCAPServices(JsonElement rawServices) throws IOException {

        JsonParser parser = new JsonParser();
        String vcapString;
        String vcapContents = null;

        if (rawServices == null || rawServices.isJsonNull()) {
            // if rawServices is null, then pull from the environment
            vcapString = System.getenv("VCAP_SERVICES");
            if (vcapString == null) {
                throw new IllegalStateException(
                        "VCAP_SERVICES are not defined, please set environment variable VCAP_SERVICES or configuration property: "
                                + VCAP_SERVICES);
            }
            // resulting string can be either the serialized JSON or filename
            if (Files.isRegularFile(Paths.get(vcapString))) {
                Path vcapFile = Paths.get(vcapString);
                vcapContents = new String(Files.readAllBytes(vcapFile), StandardCharsets.UTF_8);
            } else {
                vcapContents = vcapString;
            }
        } else if (rawServices.isJsonObject()) {
            return rawServices.getAsJsonObject();
        } else if (rawServices.isJsonPrimitive()) {
            // String can be either the serialized JSON or filename
            String rawString = rawServices.getAsString();
            if (Files.isRegularFile(Paths.get(rawString))) {
                Path vcapFile = Paths.get(rawString);
                vcapContents = new String(Files.readAllBytes(vcapFile), StandardCharsets.UTF_8);
            } else
                vcapContents = rawString;
        } else {
            throw new IllegalArgumentException("Unknown VCAP_SERVICES object class: " + rawServices.getClass());
        }
        return parser.parse(vcapContents).getAsJsonObject();
    }

    /**
     * Get the specific streaming analytics service from the service name and
     * the vcap services or the service definition.
     * 
     * 
     * @throws IOException
     */
    public static JsonObject getVCAPService(JsonObject deploy) throws IOException {
        
        JsonObject services;
        String serviceName = null;
        if (deploy.has(SERVICE_DEFINITION)) {
            JsonObject definition = GsonUtilities.object(deploy, SERVICE_DEFINITION);
            services = vcapFromServiceDefinition(definition);
            serviceName = nameFromServiceDefinition(definition);            
        } else {      
             services = getVCAPServices(deploy.get(VCAP_SERVICES));
        }

        JsonArray streamsServices = array(services, "streaming-analytics");
        if (streamsServices == null || streamsServices.size() == 0)
            throw new IllegalStateException("No streaming-analytics services defined in VCAP_SERVICES");

        if (serviceName == null)
            serviceName = jstring(deploy, SERVICE_NAME);

        // if we don't find our serviceName check the environment variable
        if (serviceName == null) {
            serviceName = System.getenv("STREAMING_ANALYTICS_SERVICE_NAME");
        }

        if (serviceName != null)
            serviceName = serviceName.trim();

        if (serviceName == null || serviceName.isEmpty())
            throw new IllegalStateException(
                    "Streaming Analytics service name is not defined, please set configuration property: "
                            + SERVICE_NAME);

        JsonObject service = null;
        for (JsonElement ja : streamsServices) {
            JsonObject possibleService = ja.getAsJsonObject();
            if (serviceName.equals(possibleService.get("name").getAsString())) {
                service = possibleService;
                break;
            }
        }

        if (service == null)
            throw new IllegalStateException(
                    "No streaming-analytics services defined in VCAP_SERVICES with name: " + serviceName);

        return service;
    }
    
    /**
     * Return a mocked up VCAP services entry that represents the single
     * service definition. Simplifies code by making the lower levels always
     * handle a VCAP_SERVICES format.
     */
    private static JsonObject vcapFromServiceDefinition(JsonObject serviceDefinition) {
        // Create a VCAP services that contains our single service
        JsonObject singleVcap = new JsonObject();
        JsonArray services = new JsonArray();
        singleVcap.add("streaming-analytics", services);

        JsonObject credentials = GsonUtilities.object(serviceDefinition, "credentials");
        if (credentials != null) {
            String type = jstring(serviceDefinition, "type");
            if (!"streaming-analytics".equals(type))
                throw new IllegalArgumentException("Invalid type for service definition, streaming-analytics required: " + type);
        } else {
            credentials = serviceDefinition;
        }
        
        JsonObject service = new JsonObject();
        service.addProperty("name", nameFromServiceDefinition(serviceDefinition));
        service.add("credentials", credentials);
        
        services.add(service);

        return singleVcap;
    }
    
    private static String nameFromServiceDefinition(JsonObject serviceDefinition) {
        String name = "service";
        if (serviceDefinition.has("credentials"))
            name = Objects.requireNonNull(jstring(serviceDefinition, "name"));
        return name;
    }
}
