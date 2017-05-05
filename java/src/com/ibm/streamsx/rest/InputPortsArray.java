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
 * Package class to hold all information about inputPorts from the GET
 * inputPorts URL
 */
class InputPortsArray {
    private InputPortsArrayGson inputPortsArray;

    public InputPortsArray(StreamsConnection sc, String gsonInputPorts) {
        inputPortsArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create()
                .fromJson(gsonInputPorts,InputPortsArrayGson.class);

        for (InputPort ip : inputPortsArray.inputPorts) {
            ip.setConnection(sc);
        }
    };

    /**
     * @return List of {@InputPort}
     */
    public List<InputPort> getInputPorts() {
        return inputPortsArray.inputPorts;
    }

    private static class InputPortsArrayGson {
        @Expose
        private ArrayList<InputPort> inputPorts;
        @Expose
        private String resourceType;
        @Expose
        private int total;
    }

    public String getResourceType() {
        return inputPortsArray.resourceType;
    }

    public int getTotal() {
        return inputPortsArray.total;
    }

}
