/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017  
 */
package com.ibm.streamsx.topology.internal.streaminganalytics;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.topology.function.Function;

/**
 * Utilities to get the correct VCAP services
 * information for submission to Streaming Analytics Service.
 *
 */
public class VcapServices {
    
    /**
     * Get the top-level VCAP services object.
     * 
     * Object can be:
     * JsonObject - assumed to contain VCAP_SERVICES
     * String -  assumed to contain serialized VCAP_SERVICES JSON
     * File - assumed to be a file containing serialized VCAP_SERVICES JSON
     */
    private static JsonObject getVCAPServices(Object rawServices) throws IOException {
        
        if (rawServices instanceof JsonObject)
            return (JsonObject) rawServices;
        
        JsonParser parser = new JsonParser();
        String vcapString;

        if (rawServices == null) {
            vcapString = System.getenv("VCAP_SERVICES");
            if (vcapString == null) {
                throw new IllegalStateException("VCAP_SERVICES are not defined, please set environment variable VCAP_SERVICES or configuration property: " + VCAP_SERVICES);
            }
        }     
        else if (rawServices instanceof File) {
            File fServices = (File) rawServices;          
            vcapString = new String(Files.readAllBytes(fServices.toPath()), StandardCharsets.UTF_8);

        } else if (rawServices instanceof JsonPrimitive) {
            vcapString = ((JsonPrimitive) rawServices).getAsString();
        }
        else if (rawServices instanceof String) {
            vcapString = rawServices.toString();
            
        } else {
            throw new IllegalArgumentException("Unknown VCAP_SERVICES object class: " + rawServices.getClass());
        }
        
        return parser.parse(vcapString).getAsJsonObject();
    }
    
    /**
     * Get the sepcific streaming analytics service from the service name
     * and the vcap services.
     * @param getter How to get the value from the container given a key

     * @throws IOException
     */
    public static JsonObject getVCAPService(Function<String,Object> getter) throws IOException {
        JsonObject services = getVCAPServices(getter.apply(VCAP_SERVICES));
        
        JsonArray streamsServices = array(services, "streaming-analytics");
        if (streamsServices == null || streamsServices.size() == 0)
            throw new IllegalStateException("No streaming-analytics services defined in VCAP_SERVICES");
        
        String serviceName = null;
        Object sno = getter.apply(SERVICE_NAME);

        if (sno instanceof JsonPrimitive)
            serviceName = ((JsonPrimitive) sno).getAsString().trim();
        else if (sno != null)
            serviceName = sno.toString().trim();
        
        if (serviceName == null || serviceName.isEmpty())
            throw new IllegalStateException("Streaming Analytics service name is not defined, please set configuration property: " + SERVICE_NAME);
                
        JsonObject service = null;
        if (!serviceName.isEmpty()) {
            for (JsonElement ja : streamsServices) {
                JsonObject possibleService = ja.getAsJsonObject();
                if (serviceName.equals(possibleService.get("name").getAsString())) {
                    service = possibleService;
                    break;
                }
            }
        }
        if (service == null)
            throw new IllegalStateException(
                    "No streaming-analytics services defined in VCAP_SERVICES with name: " + serviceName);
        
        return service;
    }
}
