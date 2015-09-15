package com.ibm.streamsx.topology.internal.core;

import java.util.HashMap;
import java.util.Map;

import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.tuple.JSONAble;

/**
 * A specification for a value of type {@code T}
 * whose actual value is not defined until topology execution time.
 */
public class SubmissionParameter<T> implements Supplier<T>, JSONAble {
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
    
    private static MetaType getMetaType(Class<?> valueClass) {
        MetaType metaType = toMetaType.get(valueClass);
        if (metaType == null)
            throw new IllegalArgumentException("Unhandled valueClass " + valueClass.getCanonicalName());
        return metaType;
    }

    /*
     * A submission time parameter specification without a default value.
     * @param name submission parameter name
     * @param valueClass class object for {@code T}
     * @throws IllegalArgumentException if {@code name} is null or empty
     */
    public SubmissionParameter(String name, Class<T> valueClass) {
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("name");
        this.name = name;
        this.metaType = getMetaType(valueClass);
        this.defaultValue = null;
    }

    /**
     * A submission time parameter specification with a default value.
     * @param name submission parameter name
     * @param defaultValue default value if parameter isn't specified.
     * @throws IllegalArgumentException if {@code name} is null or empty
     * @throws IllegalArgumentException if {@code defaultValue} is null
     */
    public SubmissionParameter(String name, T defaultValue) {
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("name");
        if (defaultValue == null)
            throw new IllegalArgumentException("defaultValue");
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
     * @param name
     * @param jvalue JSONObject
     * @param withDefault true create a submission parameter with a default value,
     *        false for one without a default value.
     *        When false, the wrapped value's value is ignored.
     */
    @SuppressWarnings("unchecked")
    public SubmissionParameter(String name, JSONObject jvalue, boolean withDefault) {
        String type = (String) jvalue.get("type");
        if (!"__spl_value".equals(type))
            throw new IllegalArgumentException("defaultValue");
        JSONObject value = (JSONObject) jvalue.get("value");
        this.name = name;
        this.defaultValue = withDefault ? (T) value.get("value") : null;
        this.metaType =  MetaType.valueOf((String) value.get("metaType"));
    }

    @Override
    public T get() {
        return null;
    }
   
    public String getName() {
        return name;
    }
    
    public T getDefaultValue() {
        return defaultValue;
    }

    @Override
    public JSONObject toJSON() {
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
        OrderedJSONObject jo = new OrderedJSONObject();
        OrderedJSONObject jv = new OrderedJSONObject();
        jo.put("type", "submissionParameter");
        jo.put("value", jv);
        jv.put("name", name);
        jv.put("metaType", metaType.name());
        jv.put("defaultValue", defaultValue);
        return jo;
    }

    @Override
    public String toString() {
        return toJSON().toString();
    }
}
