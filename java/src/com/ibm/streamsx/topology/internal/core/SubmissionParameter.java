/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SUBMISSION_PARAMETER;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonObject;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.functional.ops.SubmissionParameterManager;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * A specification for a value of type {@code T}
 * whose actual value is not defined until topology execution time.
 */
public class SubmissionParameter<T> implements Supplier<T> {
    private static final long serialVersionUID = 1L;
    private static final Map<Class<?>,MetaType> toMetaType = new HashMap<>();
    static {
        toMetaType.put(String.class,    MetaType.RSTRING);
        toMetaType.put(Boolean.class,   MetaType.BOOLEAN);
        toMetaType.put(Byte.class,      MetaType.INT8);
        toMetaType.put(Short.class,     MetaType.INT16);
        toMetaType.put(Integer.class,   MetaType.INT32);
        toMetaType.put(Long.class,      MetaType.INT64);
        toMetaType.put(Float.class,     MetaType.FLOAT32);
        toMetaType.put(Double.class,    MetaType.FLOAT64);
    }
    
    private final String name;
    private final MetaType metaType;
    private final T defaultValue;
    private transient final Topology top;
    private transient T value;
    private transient boolean initialized;
    
    private static MetaType getMetaType(Class<?> valueClass) {
        MetaType metaType = toMetaType.get(valueClass);
        if (metaType == null)
            throw new IllegalArgumentException("Unhandled valueClass " + valueClass.getCanonicalName());
        return metaType;
    }

    /*
     * A submission time parameter specification without a default value.
     * @param top the associated topology
     * @param name submission parameter name
     * @param valueClass class object for {@code T}
     * @throws IllegalArgumentException if {@code name} is null or empty
     */
    public SubmissionParameter(Topology top, String name, Class<T> valueClass) {
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("name");
        this.top = top;
        this.name = name;
        this.metaType = getMetaType(valueClass);
        this.defaultValue = null;
    }

    /**
     * A submission time parameter specification with a default value.
     * @param top the associated topology
     * @param name submission parameter name
     * @param defaultValue default value if parameter isn't specified.
     * @throws IllegalArgumentException if {@code name} is null or empty
     * @throws IllegalArgumentException if {@code defaultValue} is null
     */
    public SubmissionParameter(Topology top, String name, T defaultValue) {
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("name");
        if (defaultValue == null)
            throw new IllegalArgumentException("defaultValue");
        this.top = top;
        this.name = name;
        this.metaType = getMetaType(defaultValue.getClass());
        this.defaultValue = defaultValue;
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
    @SuppressWarnings("unchecked")
    public SubmissionParameter(Topology top, String name, JsonObject jvalue, boolean withDefault) {
        String type = jstring(jvalue, "type");
        if (!"__spl_value".equals(type))
            throw new IllegalArgumentException("defaultValue");
        JsonObject value = GsonUtilities.object(jvalue, "value");
        this.top = top;
        this.name = name;
        this.defaultValue = withDefault ? (T) value.get("value") : null;
        this.metaType =  MetaType.valueOf(jstring(value, "metaType"));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get() {
        if (!initialized) {
            if (top == null)
                value = (T) SubmissionParameterManager.getValue(name, metaType);
            initialized = true;
        }
        return value;
    }
   
    public String getName() {
        return name;
    }
    
    public T getDefaultValue() {
        return defaultValue;
    }

    public JsonObject asJSON() {
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
        jv.addProperty("name", name);
        jv.addProperty("metaType", metaType.name());
        if (defaultValue != null)
            GsonUtilities.addToObject(jv, "defaultValue", defaultValue);
        return jo;
    }

    @Override
    public String toString() {
        return asJSON().toString();
    }
}
