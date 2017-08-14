/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2017
 */
package com.ibm.streamsx.topology.builder;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * A parameter to an operator is represented by
 * a type and a value. This defines constants
 * representing the special types.
 *
 */
public interface JParamTypes {
    
    /**
     * Create a JSON representation of a parameter.
     * @param type
     * @param value
     * @return
     */
    static JsonObject create(String type, Object value) {
        JsonObject param = new JsonObject();
        
        GsonUtilities.addToObject(param, "value", value);

        if (type != null) {
            param.addProperty("type", type);
            if (JParamTypes.TYPE_ENUM.equals(type))
                param.addProperty("enumclass", value.getClass().getCanonicalName());              
        }
        
        return param;
    }
    
	/**
	 * An SPL enum value as a string.
	 */
	String TYPE_ENUM = "enum";
	
	/**
	 * An SPL schema type as a string.
	 */
	String TYPE_SPLTYPE = "spltype";
	
	/**
	 * An SPL input attribute as a string.
	 */
	String TYPE_ATTRIBUTE = "attribute";
	
	 /**
	  * Submission parameter value.
	  * 
	  * Used to declare a submission parameter at the
	  * topology level in the "parameters" object
	  * and as an operator parameter (referencing the value).
	  * 
	  * value: { name: name, metaType: type, defaultValue: value)
	  * 
	  * defaultValue is optional.
	  * 
	  */
    String TYPE_SUBMISSION_PARAMETER = "submissionParameter";
    
    /**
     * Composite parameter value.
     * 
     * Used to declare a composite parameter at the
     * composite level in the "parameters" object.
     * 
     * value: { name: name, metaType: type, defaultValue: value)
     * 
     * defaultValue is optional.
     * 
     */
    String TYPE_COMPOSITE_PARAMETER = "compositeParameter";
    
    /**
     * An SPL expression.
     */
    String TYPE_SPL_EXPRESSION = "splexpr";
}
