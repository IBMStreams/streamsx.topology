/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jint;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;

interface BPort {
    
    JsonObject _json();
    
    default void addPortInfo(int index, String name, StreamSchema schema) {
        
        _json().addProperty("name", name);
        _json().addProperty("type", schema.getLanguageType());
        _json().addProperty("index", index);
        
        _json().add("connections", new JsonArray());
    }
    
    default String name() {
        return jstring(_json(), "name");
    }
    default int index() {
        return jint(_json(), "index");
    }
    default String _schema() {
        return jstring(_json(), "type");
    }
    default StreamSchema __schema() {
        return Type.Factory.getTupleType(_schema()).getTupleSchema();
    }
    
    default void connect(BPort other) {        
        JsonPrimitive on = new JsonPrimitive(other.name());
        assert !array(_json(), "connections").contains(on);
        array(_json(), "connections").add(on);    
    }
}
