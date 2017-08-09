/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.spi.builder;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.Topology;

public class Builder {
    
    public static JsonObject graph(Topology topology) {
        return topology.builder()._json();
    }
}
