/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.generator.operator.OpProperties;

/**
 * Utility methods for an operator represented as a JSON Object.
 *
 */
public class JOperator {
    
    /**
     * Programming model used to create the operator.
     */
    public static final String MODEL = OpProperties.MODEL;
    
    /**
     * Language for the operator within its {@link #MODEL}.
     */
    public static final String LANGUAGE = OpProperties.LANGUAGE;
    
    public static final String MODEL_FUNCTIONAL = OpProperties.MODEL_FUNCTIONAL;
    public static final String MODEL_SPL = OpProperties.MODEL_SPL;
    
    public static final String LANGUAGE_JAVA = OpProperties.LANGUAGE_JAVA;
    public static final String LANGUAGE_CPP = OpProperties.LANGUAGE_CPP;
    public static final String LANGUAGE_PYTHON = OpProperties.LANGUAGE_PYTHON;
    public static final Object LANGUAGE_SPL = OpProperties.LANGUAGE_SPL;
      
    /**
     * JSON attribute for operator configuration.
     */
    public static final String CONFIG = OpProperties.CONFIG; 
    
    /**
     * Attribute for isolation region identifier.
     */
    public static final String PLACEMENT_ISOLATE_REGION_ID = OpProperties.PLACEMENT_ISOLATE_REGION_ID;

    /**
     * Attribute for an explicit colocation identifier.
     */
    public static final String PLACEMENT_EXPLICIT_COLOCATE_ID = OpProperties.PLACEMENT_EXPLICIT_COLOCATE_ID;

    /**
     * Attribute for low latency region identifier.
     */
    public static final String PLACEMENT_LOW_LATENCY_REGION_ID = OpProperties.PLACEMENT_LOW_LATENCY_REGION_ID;

    /**
     * Attribute for an resource tags, a list of tags.
     */
    public static final String PLACEMENT_RESOURCE_TAGS = OpProperties.PLACEMENT_RESOURCE_TAGS;


    
    /**
     * Get the config object, returning null if it has not been created.
     */
    public static JSONObject getConfig(final JSONObject op) {
        return (JSONObject) op.get(CONFIG);
    }
    
    public static boolean hasConfig(final JSONObject op) {
        JSONObject config = getConfig(op);
        return config != null && !config.isEmpty();
    }
    
    /**
     * Create the config object if it has not already been created.
     * @return A new or existing config object.
     */
    public static JSONObject createConfig(final JSONObject op) {
        JSONObject config = getConfig(op);
        if (config == null)
            op.put(CONFIG, config = new JSONObject());
        
        return config;
    }
    
    /**
     * Utility methods for the {@link JOperator#CONFIG} attribute
     * which is a JSON object containing arbitrary configuration items. 
     *
     */
    public static class JOperatorConfig {
        
        /**
         * Attribute for placement of the operator, a JSON object.
         */
        public static final String PLACEMENT = "placement";
        
        /**
         * Add a config value.
         * @param key Key within config
         * @param value Value, must be a JSON compatible object.
         */
        public static void addItem(final JSONObject op, String key, Object value) {
            createConfig(op).put(key, value);
        }
        
        
        /**
         * Get a config value, returning null if it has not been defined.
         */
        public static Object getItem(final JSONObject op, String key) {
            JSONObject config = getConfig(op);
            if (config == null)
                return null;
            
            return config.get(key);
        }
        
        /**
         * Get a Boolean config value, returning null if it has not been defined.
         */
        public static Boolean getBooleanItem(final JSONObject op, String key) {        
            return (Boolean) getItem(op, key);
        }
        
        /**
         * Get a String config value, returning null if it has not been defined.
         */
        public static String getStringItem(final JSONObject op, String key) {        
            return (String) getItem(op, key);
        }
        
        /**
         * Get a JSON config value, returning null if it has not been defined.
         */
        public static JSONObject getJSONItem(final JSONObject op, String key) {       
            return (JSONObject) getItem(op, key);
        }
        /**
         * Create a JSON config value if it has not already been created.
         */
        public static JSONObject createJSONItem(final JSONObject op, String key) {
            JSONObject value = (JSONObject) getItem(op, key);
            if (value == null) {
                createConfig(op).put(key, value = new JSONObject());
            }
            
            return value;
        }
    }
    
    

    

}
