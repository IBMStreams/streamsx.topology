package com.ibm.streamsx.topology;

import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.tuple.JSONAble;

/**
 * A {@link Value} whose actual value at runtime is defined
 * at topology submission time either
 * via {@link StreamsContext#submit(com.ibm.streamsx.topology.Topology, java.util.Map)}
 * and {@link ContextProperties#SUBMISSION_PARAMS}, or when submitting a topology
 * via other execution runtime native mechanisms such as 
 * IBM Streams {@code streamtool}.
 * <p>
 * Topology submission behavior when a {@code SubmissionParameter} 
 * lacking a default value is used but a value is not supplied at
 * submission time is defined by the underlying topology execution runtime.
 * Submission fails for contexts {@code DISTRIBUTED}, {@code STANDALONE},
 * or {@code ANALYTIC_SERVICE}.
 * <p>
 * Use as an SPL operator parameter value specification:
 * <pre>{@code
 * Map<String,Object> params = ...
 * 
 * // define default submission parameter "p1" for
 * // SPL op parameter "int32 myInt32"
 * params.add("myInt32", new SubmissionParameter<Integer>("p1", 100);
 * 
 * // define default submission parameter "p2" for
 * // SPL op parameter "uint32 myUint32"
 * params.add("myUint32", SubmissionParameter.newUnsigned("p2", 100);
 * 
 * // define required submission parameter "p3" for
 * // SPL op parameter "rstring myRstring"
 * params.add("myRstring", new SubmissionParameter<String>("p3", String.class);
 * 
 * ... = SPL.invokeOperator(..., params);
 * }</pre>
 * <p>
 * Mapping of {@code T} to SPL types:
 * <table>
 * <tr><td>Java type</td><td>SPL type</td><td>Alternate SPL type</td></tr>
 * <tr><td>String</td><td>rstring</td><td>ustring</td></tr>
 * <tr><td>Boolean</td><td>boolean</td></tr>
 * <tr><td>Byte</td><td>int8</td><td>uint8</td></tr>
 * <tr><td>Short</td><td>int16</td><td>uint16</td></tr>
 * <tr><td>Integer</td><td>int32</td><td>uint32</td></tr>
 * <tr><td>Long</td><td>int64</td><td>uint64</td></tr>
 * <tr><td>Float</td><td>float32</td></tr>
 * <tr><td>Double</td><td>float64</td></tr>
 * </table>
 * 
 * @see ContextProperties#SUBMISSION_PARAMS
 */
public class SubmissionParameter<T> extends Value<T> implements JSONAble {
    private final String name;
    private final Class<T> valueClass;
    private final T defaultValue;
    private boolean isAltType;

    /*
     * A submission time parameter {@code Value} specification.
     * @param name submission parameter name
     * @param valueClass class object for {@code T}
     * @throws IllegalArgumentException if {@code name} is null or empty
     */
    public SubmissionParameter(String name, Class<T> valueClass) {
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("name");
        this.name = name;
        this.valueClass = valueClass;
        this.defaultValue = null;
    }

    /**
     * A submission time parameter {@code Value} specification.
     * @param name submission parameter name
     * @param defaultValue default value if parameter isn't specified.
     * @throws IllegalArgumentException if {@code name} is null or empty
     * @throws IllegalArgumentException if {@code defaultValue} is null
     */
    @SuppressWarnings("unchecked")
    public SubmissionParameter(String name, T defaultValue) {
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("name");
        if (defaultValue == null)
            throw new IllegalArgumentException("defaultValue");
        this.name = name;
        this.valueClass = (Class<T>) (defaultValue.getClass());
        this.defaultValue = defaultValue;
    }
    
    /**
     * Create a SubmissionParameter {@code Value} for an unsigned integer type.
     * @param name submission parameter name
     * @param valueClass class object for {@code T}
     * @return SubmissionParameter for unsigned integral type
     * @throws IllegalArgumentException if {@code name} is null or empty
     * @throws IllegalArgumentException if {@code T} is not Byte, Short, Integer or Long
     */
    public static <T> SubmissionParameter<T> newUnsigned(String name, Class<T> valueClass) {
        if (valueClass != Byte.class
            && valueClass != Short.class
            && valueClass != Integer.class
            && valueClass != Long.class)
            throw new IllegalArgumentException("valueClass " + valueClass);
        SubmissionParameter<T> p = new SubmissionParameter<T>(name, valueClass);
        p.isAltType = true;
        return p;
    }

    /**
     * Create a SubmissionParameter {@code Value} for an unsigned integer type.
     * @param name submission parameter name
     * @param defaultValue default value if parameter isn't specified.
     * @return SubmissionParameter for unsigned integral type
     * @throws IllegalArgumentException if {@code name} is null or empty
     * @throws IllegalArgumentException if {@code defaultValue} is null
     * @throws IllegalArgumentException if {@code T} is not Byte, Short, Integer or Long
     */
    public static <T> SubmissionParameter<T> newUnsigned(String name, T defaultValue) {
        if (defaultValue == null)
            throw new IllegalArgumentException("defaultValue");
        if (! (defaultValue instanceof Byte
                || defaultValue instanceof Short
                || defaultValue instanceof Integer
                || defaultValue instanceof Long))
            throw new IllegalArgumentException("defaultValue class " + defaultValue.getClass());
        SubmissionParameter<T> p = new SubmissionParameter<T>(name, defaultValue);
        p.isAltType = true;
        return p;
    }

    /**
     * Create a SubmissionParameter {@code Value} for an SPL ustring type.
     * @param name submission parameter name
     * @param defaultValue default value if parameter isn't specified. May be null.
     * @return SubmissionParameter for unsigned integral type
     * @throws IllegalArgumentException if {@code name} is null or empty
     */
    public static SubmissionParameter<String> newUstring(String name, String defaultValue) {
        SubmissionParameter<String> p;
        if (defaultValue != null)
            p = new SubmissionParameter<String>(name, defaultValue);
        else
            p = new SubmissionParameter<String>(name, String.class);
        p.isAltType = true;
        return p;
    }
    
    public String getName() {
        return name;
    }
    
    public T getDefaultValue() {
        return defaultValue;
    }
    
    public boolean getIsAltType() {
        return isAltType;
    }

    @Override
    public JSONObject toJSON() {
        // meet the requirements of BOperatorInvocation.setParameter()
        // and OperatorGenerator.parameterValue()
        OrderedJSONObject jo = new OrderedJSONObject();
        OrderedJSONObject jv = new OrderedJSONObject();
        jo.put("jsonType", "submissionParameter");
        jo.put("jsonValue", jv);
        jv.put("name", name);
        jv.put("valueClassName", valueClass.getCanonicalName());
        jv.put("defaultValue", defaultValue);
        jv.put("isAltType", isAltType);
        return jo;
    }

    @Override
    public String toString() {
        return toJSON().toString();
    }
}
