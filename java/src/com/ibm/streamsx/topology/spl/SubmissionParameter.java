package com.ibm.streamsx.topology.spl;

import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.tuple.JSONAble;

/**
 * A specification for a value that is defined at topology submission time either
 * via {@link StreamsContext#submit(com.ibm.streamsx.topology.Topology, java.util.Map)
 * and {@link ContextProperties#SUBMISSION_PARAMS}, or when submitting a topology
 * via other native mechanisms such as {@code streamtool}.
 * <p>
 * TODO - also for TStream.parallel(new SubmissionParameter(...)) ?
 * <p>
 * Usage as an SPL operator parameter value specification.  
 * e.g., for an SPL operator that defines an SPL {@code int32 myOpParam}
 * operator parameter and a topology submission time parameter named
 * {@code mySubmissionParam}.
 * <pre>{@code
 * Map<String,Object> params = ...
 * params.add("myOpParam", new SubmissionParameter("mySubmissionParam", "int32");
 * ...
 * ... = SPL.invokeOperator(..., params);
 * }</pre>
 * 
 * @see ContextProperties#SUBMISSION_PARAMS
 */
public class SubmissionParameter implements JSONAble {
    private final String name;
    private final String splType;
    private final String defaultValue;

    /*
     * A submission time parameter specification.
     * @param name submission parameter name
     * @param splType SPL type string - e.g., "int32", "uint16"
     */
    public SubmissionParameter(String name, String splType) {
        this(name, splType, null);
    }

    /**
     * A submission time parameter specification.
     * @param name submission parameter name
     * @param splType SPL type string - e.g., "int32", "uint16"
     * @param defaultValue default value if parameter isn't specified. may be null.
     */
    public SubmissionParameter(String name, String splType, String defaultValue) {
        this.name = name;
        this.splType = splType;
        this.defaultValue = defaultValue;
    }

    @Override
    public JSONObject toJSON() {
        // meet the requirements of BOperatorInvocation.setParameter()
        OrderedJSONObject jo = new OrderedJSONObject();
        OrderedJSONObject jv = new OrderedJSONObject();
        jo.put("jsonType", "submissionParameter");
        jo.put("jsonValue", jv);
        jv.put("name", name);
        jv.put("splType", splType);
        jv.put("defaultValue", defaultValue);
        return jo;
    }

}
