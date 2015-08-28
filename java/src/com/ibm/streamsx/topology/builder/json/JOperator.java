package com.ibm.streamsx.topology.builder.json;

import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;

/**
 * Utility methods for an operator represented as a JSON Object.
 *
 */
public class JOperator {
    
    public static final String CONFIG = "config"; 
    
    public static final String CONFIG_PLACEMENT = "placement";
    
    public static final String PLACEMENT_COLOCATE = "colocate";

    public static final Object PLACEMENT_COLOCATE_TEMP = "colocate_TEMP";
    
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
            op.put(CONFIG, config = new OrderedJSONObject());
        
        return config;
    }
    
    /**
     * Add a config value.
     * @param key Key within config
     * @param value Value, must be a JSON compatible object.
     */
    public static void addConfig(final JSONObject op, String key, Object value) {
        createConfig(op).put(key, value);
    }
    
    
    /**
     * Get a config value, returning null if it has not been defined.
     */
    public static Object getConfigItem(final JSONObject op, String key) {
        JSONObject config = getConfig(op);
        if (config == null)
            return null;
        
        return config.get(key);
    }
    
    /**
     * Get a Boolean config value, returning null if it has not been defined.
     */
    public static Boolean getBooleanConfig(final JSONObject op, String key) {        
        return (Boolean) getConfigItem(op, key);
    }
    
    /**
     * Get a String config value, returning null if it has not been defined.
     */
    public static String getStringConfig(final JSONObject op, String key) {        
        return (String) getConfigItem(op, key);
    }
    
    /**
     * Get a JSON config value, returning null if it has not been defined.
     */
    public static JSONObject getJSONConfig(final JSONObject op, String key) {       
        return (JSONObject) getConfigItem(op, key);
    }
    /**
     * Create a JSON config value if it has not already been created.
     */
    public static JSONObject createJSONConfig(final JSONObject op, String key) {
        JSONObject value = (JSONObject) getConfigItem(op, key);
        if (value == null) {
            createConfig(op).put(key, value = new OrderedJSONObject());
        }
        
        return value;
    }
}
