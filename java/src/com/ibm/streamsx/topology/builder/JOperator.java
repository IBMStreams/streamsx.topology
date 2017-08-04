/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.CONFIG;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.addToObject;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jisEmpty;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectCreate;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * Utility methods for an operator represented as a JSON Object.
 *
 */
public class JOperator {
    
    /**
     * Get the config object, returning null if it has not been created.
     */
    public static JsonObject getConfig(final JsonObject op) {
        return GsonUtilities.object(op, CONFIG);
    }
    
    public static boolean hasConfig(final JsonObject op) {
        return jisEmpty(getConfig(op));
    }
    
    /**
     * Create the config object if it has not already been created.
     * @return A new or existing config object.
     */
    public static JsonObject createConfig(final JsonObject op) {
        return GsonUtilities.objectCreate(op, CONFIG);
    }
    
    /**
     * Utility methods for the {@link JOperator#CONFIG} attribute
     * which is a JSON object containing arbitrary configuration items. 
     *
     */
    public static class JOperatorConfig {
       
        /**
         * Add a config value.
         * @param key Key within config
         * @param value Value, must be a JSON compatible object.
         */
        public static void addItem(final JsonObject op, String property, Object value) {
            addToObject(createConfig(op), property, value);
        }
        
        
        /**
         * Get a config value, returning null if it has not been defined.
         */
        public static JsonElement getItem(final JsonObject op, String key) {
            JsonObject config = getConfig(op);
            if (config == null)
                return null;
            
            return config.get(key);
        }
        
        /**
         * Get a Boolean config value, returning null if it has not been defined.
         */
        public static Boolean getBooleanItem(final JsonObject op, String key) {
            JsonElement value = getItem(op, key);
            return value == null ? null : value.getAsBoolean();
        }
        
        /**
         * Get a String config value, returning null if it has not been defined.
         */
        public static String getStringItem(final JsonObject op, String key) {        
            JsonElement value = getItem(op, key);
            return value == null ? null : value.getAsString();
        }
        
        /**
         * Get a JSON config value, returning null if it has not been defined.
         */
        public static JsonObject getJSONItem(final JsonObject op, String key) {       
            JsonElement value = getItem(op, key);
            return value == null ? null : value.getAsJsonObject();
        }
        /**
         * Create a JSON config value if it has not already been created.
         */
        public static JsonObject createJSONItem(final JsonObject op, String property) {
            return objectCreate(createConfig(op), property);
        }
    }
}
