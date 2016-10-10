/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.generator.spl;

import java.util.Collection;
import java.util.Iterator;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.function.Consumer;

public class GsonUtilities {
    /**
     * Perform an action on every JsonObject in an array.
     */
    public static void objectArray(JsonObject object, String property, Consumer<JsonObject> action) {
        JsonArray array = array(object, property);
        if (array == null)
            return;
        array.forEach(e -> action.accept(e.getAsJsonObject()));
    }
    
    /**
     * Perform an action on every String in an array.
     */
    public static void stringArray(JsonObject object, String property, Consumer<String> action) {
        JsonArray array = array(object, property);
        if (array == null)
            return;
        array.forEach(e -> action.accept(e.getAsString()));
    }
    
    /**
     * Return a Json array. If the value is not
     * an array then an array containing the single
     * value is returned.
     * Returns null if the array is empty or not present.
     */
    static JsonArray array(JsonObject object, String property) {
        if (object.has(property)) {
            JsonElement je = object.get(property);
            if (je.isJsonNull())
                return null;
            if (je.isJsonArray())
                return je.getAsJsonArray();
            JsonArray array = new JsonArray();
            array.add(je);
            return array;
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
    
    static boolean jisEmpty(JsonObject object) {
        return object == null || object.isJsonNull() || object.entrySet().isEmpty();
    }
    static boolean jisEmpty(JsonArray array) {
        return array == null || array.size() == 0;
    }
    
    static void gclear(JsonArray array) {
        Iterator<JsonElement> it = array.iterator();
        while(it.hasNext())
            it.remove();
    }
    
    
    public static String jstring(JsonObject object, String property) {
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
    
    static JsonObject first(Collection<JsonObject> objects) {
        return objects.iterator().next();
    }
    
    static JsonObject nestedObject(JsonObject object, String nested, String property) {
        JsonObject nester = jobject(object, nested);
        if (nester == null)
            return null;
        
        return jobject(nester, property);
    }
    
    public static JsonObject object(JsonObject object,  String ...property) {
        
        assert property.length > 0;
        
        JsonObject item = null;
        for (String key : property) {
            item = jobject(object, key);
            if (item == null)
                return null;
            object = item;
        }

        return item; 
    }
    
    public static JsonObject objectCreate(JsonObject object, String ...property) {
        
        assert property.length > 0;
        
        JsonObject item = null;
        for (String key : property) {
            item = jobject(object, key);
            if (item == null)
                object.add(key, item = new JsonObject());
            object = item;
        }

        return item; 
    }
    
    
    static JsonObject nestedObjectCreate(JsonObject object, String nested, String property) {
        return objectCreate(object, nested, property);
    }
}
