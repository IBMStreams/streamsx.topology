package com.ibm.streamsx.topology.generator.spl;

import java.util.HashMap;
import java.util.Map;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streams.operator.Type.MetaType;

/**
 * A Submission Time Value is the SPL realization of a "Submission Parameter".
 */
public class SubmissionTimeValue {
    private static final Map<String,String> javaToSPL = new HashMap<>();
    static {
        javaToSPL.put("java.lang.Boolean", "boolean");
        javaToSPL.put("java.lang.String", "rstring");
        javaToSPL.put("java.lang.Byte", "int8");
        javaToSPL.put("java.lang.Short", "int16");
        javaToSPL.put("java.lang.Integer", "int32");
        javaToSPL.put("java.lang.Long", "int64");
        javaToSPL.put("java.lang.Float", "float32");
        javaToSPL.put("java.lang.Double", "float64");
    }
    
    /**
     * Create a json operator parameter name for the submission parameter name. 
     * @param spName the submission parameter name
     * @return the name
     */
    public static String mkOpParamName(String spName) {
        return "__jaa_stv_" + spName;
    }

    /**
     * Enrich the json composite operator definition's parameters
     * to include parameters for submission parameters used within the composite.
     * @param composite the composite definition
     */
    static void addJsonParamDefs(JSONObject composite) {
        // scan immediate children ops for submission param use
        // and add corresponding param definitions to the composite
        
        // scan for spParams
        JSONObject spParams = new JSONObject();
        JSONArray operators = (JSONArray) composite.get("operators");
        for (Object op : operators) {
            JSONObject jop = (JSONObject)op;
            JSONObject params = (JSONObject) jop.get("parameters");
            if (params != null) {
                for (Object pname : params.keySet()) {
                    JSONObject param = (JSONObject) params.get(pname);
                    String type = (String) param.get("type");
                    if ("submissionParameter".equals(type)) {
                        JSONObject spval = (JSONObject) param.get("value");
                        pname = mkOpParamName((String)spval.get("name"));
                        spParams.put(pname, param);
                    }
                }
            }
            Boolean isParallel = (Boolean) jop.get("parallelOperator"); 
            if (isParallel != null && isParallel) {
                Object width = jop.get("width");
                if (width instanceof JSONObject) {
                    JSONObject jwidth = (JSONObject)width; 
                    Object type = jwidth.get("type");
                    if ("submissionParameter".equals(type)) {
                        JSONObject spval = (JSONObject) jwidth.get("value");
                        String pname = mkOpParamName((String)spval.get("name")); 
                        spParams.put(pname, jwidth);
                    }
                }
            }
        }
        
        // augment the parameters
        JSONObject params = (JSONObject) composite.get("parameters");
        if (params == null && spParams.size() > 0) {
            params = new OrderedJSONObject();
            composite.put("parameters", params);
        }
        for (Object pname : spParams.keySet()) {
            if (!params.keySet().contains(pname))
                params.put(pname, spParams.get(pname));
        }
        
        // make the results of our efforts available to addJsonInstanceParams
        composite.put("__spl_submissionParams", spParams);
    }

    /**
     * Akin to addJsonParamDefs(), enrich the json composite operator instance's
     * parameters with submission parameter references.
     * @param compInstance the composite instance
     * @param composite the composite definition
     */
    static void addJsonInstanceParams(JSONObject compInstance, JSONObject composite) {
        JSONObject spParams = (JSONObject) composite.get("__spl_submissionParams");
        if (spParams != null) {
            JSONObject opParams = (JSONObject) compInstance.get("parameters");
            if (opParams == null) {
                opParams = new JSONObject();
                compInstance.put("parameters", opParams);
            }
            for (Object pname : spParams.keySet()) {
                JSONObject spParam = (JSONObject) spParams.get(pname);
                // need to end up generating: __jaa_stv_foo : $__jaa_stv_foo;
                JSONObject spval = (JSONObject) spParam.get("value");
                pname = mkOpParamName((String)spval.get("name")); 
                opParams.put(pname, spParam);
            }
        }
    }
    
    /**
     * Generate a submission time value SPL param value definition
     * in a main composite.
     * <p>
     * e.g.,
     * <pre>{@code
     *  param
     *      expression<uint32> $__jaa_stv_foo : (uint32) getSubmissionTimeValue("foo", 3)
     * }</pre>
     * @param spval JSONObject for the submission parameter's value
     * @param sb
     */
    static void generateMainDef(JSONObject spval, StringBuilder sb) {
        String splName = "$" + mkOpParamName((String)spval.get("name"));
        String name = SPLGenerator.stringLiteral((String) spval.get("name"));
        String metaType = (String) spval.get("metaType");
        String splType = MetaType.valueOf(metaType.toUpperCase()).getLanguageType();
        Object defaultValue = spval.get("defaultValue");
        sb.append(String.format("expression<%s> %s : ", splType, splName));
        if (defaultValue == null)
            sb.append(String.format("(%s) getSubmissionTimeValue(%s)", splType, name));
        else {
            if (splType.startsWith("uint"))
                defaultValue = SPLGenerator.unsignedString(defaultValue);
            defaultValue = SPLGenerator.stringLiteral(defaultValue.toString());
            sb.append(String.format("(%s) getSubmissionTimeValue(%s, %s)", splType, name, defaultValue));
        }
    }
    
    /**
     * Generate a submission time value SPL param value definition
     * in an inner (non-main) composite definition.
     * <p>
     * e.g.,
     * <pre>{@code
     *  param
     *      expression<uint32> $__jaa_stv_foo
     * }</pre>
     * @param spval JSONObject for the submission parameter's value
     * @param sb
     */
    static void generateInnerDef(JSONObject spval, StringBuilder sb) {
        String name = "$" + mkOpParamName((String)spval.get("name"));
        String metaType = (String) spval.get("metaType");
        String splType = MetaType.valueOf(metaType.toUpperCase()).getLanguageType();
        sb.append(String.format("expression<%s> %s", splType, name));
    }
    
    /**
     * Generate a submission time value SPL operator param reference 
     * <p>
     * <pre><code>
     *  param
     *      ... : $__jaa_stv_foo
     *      
     *  {@literal @}parallel(width=$__jaa_stv_foo)
     * </code></pre>
     * @param spval JSONObject for the submission parameter's value
     * @param sb
     */
    static void generateRef(JSONObject spval, StringBuilder sb) {
        String ref = "$" + mkOpParamName((String)spval.get("name"));
        sb.append(ref);
    }

}
