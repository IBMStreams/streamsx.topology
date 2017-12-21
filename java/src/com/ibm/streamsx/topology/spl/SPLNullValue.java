/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.JParamTypes;

/**
 * An implementation private wrapper for null values of an SPL optional type. 
 * <p>
 * See {@link SPL#createNullValue()
 */
class SPLNullValue {

    static JsonObject asJSON() {
        JsonObject jo = new JsonObject();
        jo.addProperty("type", JParamTypes.TYPE_SPL_EXPRESSION);
        jo.addProperty("value", "null");
        return jo;
    }
}
