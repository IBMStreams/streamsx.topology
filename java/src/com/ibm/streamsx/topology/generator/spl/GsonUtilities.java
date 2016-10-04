/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.generator.spl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.function.Consumer;

public class GsonUtilities {
    /**
     * Perform an action on every JsonObject in an array.
     */
    static void objectArray(JsonObject object, String property, Consumer<JsonObject> action) {
        JsonArray array = array(object, property);
        if (array == null)
            return;
        array.forEach(e -> action.accept(e.getAsJsonObject()));
    }
    
    /**
     * Perform an action on every String in an array.
     */
    static void stringArray(JsonObject object, String property, Consumer<String> action) {
        JsonArray array = array(object, property);
        if (array == null)
            return;
        array.forEach(e -> action.accept(e.getAsString()));
    }
    
    /**
     * Return a Json array.
     * Returns null if the array is empty or not present.
     */
    static JsonArray array(JsonObject object, String property) {
        if (object.has(property)) {
            JsonElement je = object.get(property);
            if (je.isJsonNull())
                return null;
            return je.getAsJsonArray();
        }
        return null;
    }
    /**
     * Return a Json object.
     * Returns null if the object is not present or null.
     */
    static JsonObject jobject(JsonObject object, String property) {
        if (object.has(property)) {
            JsonElement je = object.get(property);
            if (je.isJsonNull())
                return null;
            return je.getAsJsonObject();
        }
        return null;
    }
    static String jstring(JsonObject object, String property) {
        if (object.has(property)) {
            JsonElement je = object.get(property);
            if (je.isJsonNull())
                return null;
            return je.getAsString();
        }
        return null;
    }
    static boolean jboolean(JsonObject object, String property) {
        if (object.has(property)) {
            JsonElement je = object.get(property);
            if (je.isJsonNull())
                return false;
            return je.getAsBoolean();
        }
        return false;
    }
}
