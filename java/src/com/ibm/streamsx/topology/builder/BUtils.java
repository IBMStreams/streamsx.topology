/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.flow.declare.PortDeclaration;

public class BUtils {

    public static void addPortInfo(JSONObject json, PortDeclaration<?> port) {
        json.put("name", port.getName());
        json.put("type", port.getStreamSchema().getLanguageType());
        json.put("index", port.getPortNumber());
    }
}
