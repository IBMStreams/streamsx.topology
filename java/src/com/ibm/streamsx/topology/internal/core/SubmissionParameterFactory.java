/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SUBMISSION_PARAMETER;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.functional.SPLTypes;
import com.ibm.streamsx.topology.internal.functional.SubmissionParameter;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.messages.Messages;

/**
 * Utility to split runtime submission parameters from declaration time.
 *
 */
public class SubmissionParameterFactory {
    private static final Map<Class<?>,String> toMetaType = new HashMap<>();
    static {
        toMetaType.put(String.class,    SPLTypes.RSTRING);
        toMetaType.put(Boolean.class,   SPLTypes.BOOLEAN);
        toMetaType.put(Byte.class,      SPLTypes.INT8);
        toMetaType.put(Short.class,     SPLTypes.INT16);
        toMetaType.put(Integer.class,   SPLTypes.INT32);
        toMetaType.put(Long.class,      SPLTypes.INT64);
        toMetaType.put(Float.class,     SPLTypes.FLOAT32);
        toMetaType.put(Double.class,    SPLTypes.FLOAT64);
    }
    
    public static <T> SubmissionParameter<T> create(String name, Class<T> valueClass) {
        return new SubmissionParameter<T>(name, getMetaType(valueClass), null);
    }
    public static <T> SubmissionParameter<T> create(String name, T defaultValue) {
        Objects.requireNonNull(defaultValue);
        return new SubmissionParameter<T>(name, getMetaType(defaultValue.getClass()), defaultValue);
    }
    private static String getMetaType(Class<?> valueClass) {
        String metaType = toMetaType.get(valueClass);
        if (metaType == null)
            throw new IllegalArgumentException(Messages.getString("CORE_UNHANDLED_VALUECLASS", valueClass.getCanonicalName()));
        return metaType;
    }
    
    /**
     * A submission time parameter specification with or without a default value.
     * <p>
     * The {@code jvalue} parameter must be a Value object.
     * The Value object is
     * <pre><code>
     * object {
     *   type : "__spl_value"
     *   value : object {
     *     value : any. non-null. type appropriate for metaType
     *     metaType : com.ibm.streams.operator.Type.MetaType.name() string.
     *   }
     * }
     * </code></pre>
     * @param top the associated topology
     * @param name
     * @param jvalue JSONObject
     * @param withDefault true create a submission parameter with a default value,
     *        false for one without a default value.
     *        When false, the wrapped value's value is ignored.
     */
    public static SubmissionParameter<Object> create(String name, JsonObject jvalue, boolean withDefault) {
        String type = jstring(jvalue, "type");
        if (!"__spl_value".equals(type))
            throw new IllegalArgumentException("defaultValue");
        JsonObject value = GsonUtilities.object(jvalue, "value");
        
        return new SubmissionParameter<Object>(name, jstring(value, "metaType"),
                withDefault ? value.get("value") : null);
    }
    
    public static JsonObject asJSON(SubmissionParameter<?> param) {
        // meet the requirements of BOperatorInvocation.setParameter()
        // and OperatorGenerator.parameterValue()
        /*
         * The SubmissionParameter parameter value json is: 
         * <pre><code>
         * object {
         *   type : "submissionParameter"
         *   value : object {
         *     name : string. submission parameter name
         *     metaType : operator.Type.MetaType.name() string
         *     defaultValue : any. may be null. type appropriate for metaType.
         *   }
         * }
         * </code></pre>
         */
        JsonObject jo = new JsonObject();
        JsonObject jv = new JsonObject();
        jo.addProperty("type", TYPE_SUBMISSION_PARAMETER);
        jo.add("value", jv);
        jv.addProperty("name", param.getName());
        jv.addProperty("metaType", param.getMetaType());
        if (param.getDefaultValue() != null)
            GsonUtilities.addToObject(jv, "defaultValue", param.getDefaultValue());
        return jo;
    }
}
