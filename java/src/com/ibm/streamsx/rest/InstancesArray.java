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
 * class to hold information about instances from GET instances URL
 */
class InstancesArray {
    private InstancesArrayGson instanceArray;

    public InstancesArray(StreamsConnection sc, String gsonInstances) {
        instanceArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create()
                                         .fromJson(gsonInstances, InstancesArrayGson.class);

        for (Instance in : instanceArray.instances) {
            in.setConnection(sc);
        }
    };

    /**
     * @return List of {@Instance}
     */
    public List<Instance> getInstances() {
        return instanceArray.instances;
    }

    private static class InstancesArrayGson {
        @Expose
        public ArrayList<Instance> instances;
        @Expose
        public String resourceType;
        @Expose
        public int total;
    }

    public String getResourceType() {
        return instanceArray.resourceType;
    }

    public int getTotal() {
        return instanceArray.total;
    }

}
