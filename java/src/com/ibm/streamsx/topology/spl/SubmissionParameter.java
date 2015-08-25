package com.ibm.streamsx.topology.spl;

import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.tuple.JSONAble;

/**
 * A specification for a value that is defined at topology submission time either
 * via {@link StreamsContext#submit(com.ibm.streamsx.topology.Topology, java.util.Map)}
 * and {@link ContextProperties#SUBMISSION_PARAMS}, or when submitting a topology
 * via other execution runtime native mechanisms such as {@code streamtool}.
 * <p>
 * Topology submission behavior when a submission parameter lacking a default
 * value has been declared but a value is not supplied at submission time 
 * is defined by the underlying topology execution runtime.  Submission fails
 * for contexts {@code DISTRIBUTED}, {@code STANDALONE}, or {@ANALYTIC_SERVICE}.
 * <p>
 * TODO - also for TStream.parallel(new SubmissionParameter(...)) ?
 * <p>
 * Usage as an SPL operator parameter value specification.  
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
 * <tr><td>Java</td><td>SPL</td></tr>
 * <tr><td>String</td><td>rstring</td></tr>
 * <tr><td>Boolean</td><td>boolean</td></tr>
 * <tr><td>Byte</td><td>int8, uint8</td></tr>
 * <tr><td>Short</td><td>int16, uint16</td></tr>
 * <tr><td>Integer</td><td>int32, uint32</td></tr>
 * <tr><td>Long</td><td>int64, uint64</td></tr>
 * <tr><td>Float</td><td>float32</td></tr>
 * <tr><td>Double</td><td>float64</td></tr>
 * </table>
 * 
 * @see ContextProperties#SUBMISSION_PARAMS
 */
public class SubmissionParameter<T> implements JSONAble {
    private final String name;
    private final Class<T> valueClass;
    private final Object defaultValue;
    private boolean isUnsigned;

    /*
     * A submission time parameter specification.
     * @param name submission parameter name
     * @param valueClass class object for {@code T}
     */
    public SubmissionParameter(String name, Class<T> valueClass) {
        this.name = name;
        this.valueClass = valueClass;
        this.defaultValue = null;
    }

    /**
     * A submission time parameter specification.
     * @param name submission parameter name
     * @param defaultValue default value if parameter isn't specified.
     * @throws IllegalArgumentException if {@code defaultValue} is null
     */
    @SuppressWarnings("unchecked")
    public SubmissionParameter(String name, T defaultValue) {
        if (defaultValue == null)
            throw new IllegalArgumentException("defaultValue");
        this.name = name;
        this.valueClass = (Class<T>) (defaultValue.getClass());
        this.defaultValue = defaultValue;
    }
    
    /**
     * Create a SubmissionParameter for an unsigned integer type.
     * @param name submission parameter name
     * @param valueClass class object for {@code T}
     * @return SubmissionParameter for unsigned integral type
     * @throws IllegalArgumentException if {@code T} is not Byte, Short, Integer or Long
     */
    public static <T> SubmissionParameter<T> newUnsigned(String name, Class<T> valueClass) {
        if (valueClass != Byte.class
            || valueClass != Short.class
            || valueClass != Integer.class
            || valueClass != Long.class)
            throw new IllegalArgumentException("valueClass");
        SubmissionParameter<T> p = new SubmissionParameter<T>(name, valueClass);
        p.isUnsigned = true;
        return p;
    }

    /**
     * Create a SubmissionParameter for an unsigned integer type.
     * @param name submission parameter name
     * @param defaultValue default value if parameter isn't specified.
     * @return SubmissionParameter for unsigned integral type
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
            throw new IllegalArgumentException("defaultValue");
        SubmissionParameter<T> p = new SubmissionParameter<T>(name, defaultValue);
        p.isUnsigned = true;
        return p;
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
        jv.put("isUnsigned", isUnsigned);
        return jo;
    }

}
