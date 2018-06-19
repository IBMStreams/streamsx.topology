/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.gson;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.topology.function.Consumer;

public class GsonUtilities {
    
    private static final Gson gson = new Gson();
    
    public static Gson gson() {
        return gson;
    }
    
    public static String toJson(JsonElement element) {
        return gson().toJson(element);
    }
    
    /**
     * Add value to o as property, converting as needed.
     * Supports JsonElment,String,Number,Boolean, Collection{String,Number,Boolean}
     */
    public static void addToObject(JsonObject o, String property, Object value) {
        if (value instanceof JsonElement)
            o.add(property, (JsonElement) value);
        else if (value instanceof String)
            o.addProperty(property,(String) value);
        else if (value instanceof Number)
            o.addProperty(property,(Number) value);
        else if (value instanceof Boolean)
            o.addProperty(property,(Boolean) value);
        else if (value instanceof Collection) {
            JsonArray sa = new JsonArray();
            Collection<?> values = (Collection<?>) value;
            for (Object ov : values) {
                if (ov instanceof JsonElement)
                    sa.add((JsonElement) ov);
                else if (ov instanceof String)
                    sa.add(new JsonPrimitive((String) ov));
                else if (ov instanceof Number)
                    sa.add(new JsonPrimitive((Number) ov));
                else if (ov instanceof Boolean)
                    sa.add(new JsonPrimitive((Boolean) ov));
                else
                    throw new UnsupportedOperationException("JSON:" + ov.getClass());  
            }
            
            o.add(property, sa);
        }
        else
            throw new UnsupportedOperationException("JSON:" + value.getClass());          
    }
    
    /**
     * Perform an action on every JsonObject in an array.
     */
    public static void objectArray(JsonObject object, String property, Consumer<JsonObject> action) {
        if (object == null)
            return;
        JsonArray array = array(object, property);
        if (array == null)
            return;
        array.forEach(e -> action.accept(e.getAsJsonObject()));
    }
    
    /**
     * Perform an action on every String in an array.
     */
    public static void stringArray(JsonObject object, String property, Consumer<String> action) {
        if (object == null)
            return;

        JsonArray array = array(object, property);
        if (array == null)
            return;
        array.forEach(e -> action.accept(e.getAsString()));
    }
    
    /**
     * Return a Json array. If the value is not
     * an array then an array containing the single
     * value is returned.
     * Returns null if the array is not present or present as JSON null.
     */
    public static JsonArray array(JsonObject object, String property) {
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
    
    public static boolean intersect(JsonArray a1, JsonArray a2) {
        for (JsonElement e : a1) {
            if (a2.contains(e))
                return true;
        }
        return false;
    }
    
    
    /**
     * Return a Json object.
     * Returns null if the object is not present or null.
     */
    public static JsonObject jobject(JsonObject object, String property) {
        if (object.has(property)) {
            JsonElement je = object.get(property);
            if (je.isJsonNull())
                return null;
            return je.getAsJsonObject();
        }
        return null;
    }
    
    /**
     * Return is empty meaning null, JSON null or an empty object.
     */
    public static boolean jisEmpty(JsonObject object) {
        return object == null || object.isJsonNull() || object.entrySet().isEmpty();
    }
    public static boolean jisEmpty(JsonArray array) {
        return array == null || array.size() == 0;
    }
    
    public static void gclear(JsonArray array) {
        Iterator<JsonElement> it = array.iterator();
        while(it.hasNext())
            it.remove();
    }
    
    /**
     * Returns a property as a String.
     * @param object
     * @param property
     * @return Value or null if it is not set.
     */
    public static String jstring(JsonObject object, String property) {
        if (object.has(property)) {
            JsonElement je = object.get(property);
            if (je.isJsonNull())
                return null;
            return je.getAsString();
        }
        return null;
    }
    public static boolean jboolean(JsonObject object, String property) {
        if (object.has(property)) {
            JsonElement je = object.get(property);
            if (je.isJsonNull())
                return false;
            return je.getAsBoolean();
        }
        return false;
    }
    public static int jint(JsonObject object, String property) {
        return object.get(property).getAsInt();
    }
    
    public static JsonObject first(Collection<JsonObject> objects) {
        return objects.iterator().next();
    }
    
    public static JsonObject nestedObject(JsonObject object, String nested, String property) {
        JsonObject nester = jobject(object, nested);
        if (nester == null)
            return null;
        
        return jobject(nester, property);
    }
    
    
    public static boolean hasAny(JsonObject object, Collection<String> coll){
        for(String key : coll){
            if(object.has(key))
                return true;
        }
        return false;
    }
    /**
     * Get a json object from a property or properties.
     * @param object
     * @param property
     * @return Valid object of null if any element of the properties does not exist.
     */
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
    
    /**
     * Create nested set of JSON objects in object.
     * 
     *  E.g. if passed obj, "a", "b", "c" then obj contain:
     *  
     *  "a": { "b": { "c" : {} } } }
     *  
     *  If any of the properties already exist then they must be objects
     *  then they are not modifed at that level. E.g. if "a" already exists
     *  and has "b" then it is not modified, but "b" will have "c" added to it
     *  if it didn't already exist. 
     */
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
    
    /**
     * Like objectCreate but the last element is created as an array if
     * it doesn't already exist.
     */
    public static JsonArray arrayCreate(JsonObject object, String ...property) {
        
        assert property.length > 0;
        
        if (property.length > 1) {
            String[] objprops = new String[property.length - 1];
            System.arraycopy(property, 0, objprops, 0, objprops.length);
            object = objectCreate(object, objprops);
        }
        
        String arrayProperty = property[property.length - 1];
        
        if (object.has(arrayProperty))
            return object.getAsJsonArray(arrayProperty);
        
        JsonArray array = new JsonArray();
        object.add(arrayProperty, array);
        return array;
    }
    
    /**
     * Add all the properties in the {@code source} JSON object into {@code target} JSON object. Existing properties will be overridden.
     * <p>
     * E.g. if {@code target} contains:
     * <pre><code>
     * 		{ "t1": {}, "t2": {} }
     * </code></pre>
     * and {@code source} contains:
     * <pre><code>
     * 		{ "s1": {}, "s2": {}, "s3": {} }
     * </code></pre>
     * then {@code target} after the call, contains:
     * <pre><code>
     * 		{ "t1": {}, "t2": {}, "s1": {}, "s2": {}, "s3": {} }
     * </code></pre>
     * 
     * @param target JSON object to copy properties to
     * @param source JSON object to receive properties from
     * @return modified target JSON object
     */
    public static JsonObject addAll(JsonObject target, JsonObject source) {
    	for (Entry<String, JsonElement> entry : source.entrySet()) {
    		target.add(entry.getKey(), entry.getValue());
        }
    	return target;
    }
}
