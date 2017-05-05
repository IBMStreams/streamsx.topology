/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * Package class to hold all information about outputPorts from the GET
 * outputPorts URL
 */
class OutputPortsArray {
    private OutputPortsArrayGson outputPortsArray;

    public OutputPortsArray(StreamsConnection sc, String gsonOutputPorts) {
        outputPortsArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create()
                .fromJson(gsonOutputPorts,OutputPortsArrayGson.class);

        for (OutputPort ip : outputPortsArray.outputPorts) {
            ip.setConnection(sc);
        }
    };

    /**
     * @return List of {@OutputPort}
     */
    public List<OutputPort> getOutputPorts() {
        return outputPortsArray.outputPorts;
    }

    private static class OutputPortsArrayGson {
        @Expose
        private ArrayList<OutputPort> outputPorts;
        @Expose
        private String resourceType;
        @Expose
        private int total;
    }

    public String getResourceType() {
        return outputPortsArray.resourceType;
    }

    public int getTotal() {
        return outputPortsArray.total;
    }

}
