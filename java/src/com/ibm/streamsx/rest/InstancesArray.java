/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

/**
 * class to hold information about instances from GET instances URL
 */
class InstancesArray {
    private List<Instance> instances;
    private InstancesArrayGson instanceArray;

    public InstancesArray(StreamsConnection sc, String gsonInstances) {
        instanceArray = new Gson().fromJson(gsonInstances, InstancesArrayGson.class);

        instances = new ArrayList<Instance>(instanceArray.instances.size());
        for (InstanceGson ig : instanceArray.instances) {
            instances.add(new Instance(sc, ig));
        }
    };

    /**
     * @return
     */
    public List<Instance> getInstances() {
        return instances;
    }

    private static class InstancesArrayGson {
        public ArrayList<InstanceGson> instances;
        public String resourceType;
        public int total;
    }

    public String getResourceType() {
        return instanceArray.resourceType;
    }

    public int getTotal() {
        return instanceArray.total;
    }

}
