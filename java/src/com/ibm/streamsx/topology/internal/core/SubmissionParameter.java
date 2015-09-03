package com.ibm.streamsx.topology.internal.core;

import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.tuple.JSONAble;

/**
 * A specification for a value of type {@code T}
 * whose actual value is not defined until topology execution time.
 */
public class SubmissionParameter<T> implements Supplier<T>, JSONAble {
    private static final long serialVersionUID = 1L;
    private final String name;
    private final String valueClassName;
    private final String typeModifier;
    private final T defaultValue;

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
        this.valueClassName = valueClass.getCanonicalName();
        this.typeModifier = null;
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
        this.valueClassName = defaultValue.getClass().getCanonicalName();
        this.typeModifier = null;
        this.defaultValue = defaultValue;
    }
    
    /**
     * A submission time parameter specification with or without a default value.
     * <p>
     * The wrappedValue parameter is a WrappedValue parameter value object.
     * The WrappedValue parameter value object is
     * <pre><code>
     * object {
     *   type : "wrappedValue"
     *   value : object {
     *     value : any. non-null.
     *     typeModifier : optional null, "utf16", "unsigned"
     *   }
     * }
     * </code></pre>
     * @param name
     * @param wrappedValue JSONObject
     * @param withDefault true create a submission parameter with a default value,
     *        false for one without a default value.
     *        When false, the wrapped value's value is ignored.
     */
    @SuppressWarnings("unchecked")
    public SubmissionParameter(String name, JSONObject wrappedValue, boolean withDefault) {
        String type = (String) wrappedValue.get("type");
        if (!"wrappedValue".equals(type))
            throw new IllegalArgumentException("defaultValue");
        JSONObject value = (JSONObject) wrappedValue.get("value");
        this.name = name;
        this.defaultValue = withDefault ? (T) value.get("value") : null;
        this.valueClassName = value.get("value").getClass().getCanonicalName();
        this.typeModifier = (String) value.get("typeModifier");
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
         *     valueClassName : string. a java type
         *     typeModifier : optional string to modify interpretation of
         *                      valueClassName.  "utf16", "unsigned", null 
         *     defaultValue : any. an instance of valueClassName. may be null.
         *   }
         * }
         * </code></pre>
         */
        OrderedJSONObject jo = new OrderedJSONObject();
        OrderedJSONObject jv = new OrderedJSONObject();
        jo.put("type", "submissionParameter");
        jo.put("value", jv);
        jv.put("name", name);
        jv.put("valueClassName", valueClassName);
        if (typeModifier != null)
            jv.put("typeModifier", typeModifier);
        jv.put("defaultValue", defaultValue);
        return jo;
    }

    @Override
    public String toString() {
        return toJSON().toString();
    }
}
