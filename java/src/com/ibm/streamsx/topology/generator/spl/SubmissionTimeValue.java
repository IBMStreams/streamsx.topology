package com.ibm.streamsx.topology.generator.spl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;

class SubmissionTimeValue {
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
     * Enrich the json parameters of the composite operator to include
     * parameters for submission parameters used within the composite.
     * @param composite
     */
    static void addJsonParamDefs(JSONObject composite) {
        // scan immediate children ops for submission param use
        // and add corresponding param definitions to the composite
        Map<String,JSONObject> spParams = new HashMap<>();
        JSONArray operators = (JSONArray) composite.get("operators");
        for (Object op : operators) {
            JSONObject jop = (JSONObject)op;
            JSONObject params = (JSONObject) jop.get("parameters");
            if (params != null) {
                @SuppressWarnings("unchecked")
                Set<Entry<String,Object>> paramSet = params.entrySet();
                for (Map.Entry<String, Object> param : paramSet) {
                    Object value = param.getValue();
                    if (value instanceof JSONObject) {
                        JSONObject jval = (JSONObject)value;
                        Object type = jval.get("type");
                        if ("submissionParameter".equals(type)) {
                            spParams.put(param.getKey(), jval);
                        }
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
                        String name = "__jaa_stv_" + spval.get("name"); 
                        spParams.put(name, jwidth);
                    }
                }
            }
        }
        JSONObject params = (JSONObject) composite.get("parameters");
        for (String pname : spParams.keySet()) {
            if (!params.keySet().contains(pname)) {
                JSONObject param = new JSONObject();
                param.put(pname, spParams.get(pname));
            }
        }
    }
    
    /**
     * Generate a submission time value SPL param definition in a main composite
     * @param spval JSONObject for the submission parameter's value
     * @param sb
     */
    static void generateMainDef(JSONObject spval, StringBuilder sb) {
        String splName = "$__jaa_stv_" + (String) spval.get("name");
        String name = SPLGenerator.stringLiteral((String) spval.get("name"));
        String valueClassName = (String) spval.get("valueClassName");
        String typeModifier = (String) spval.get("typeModifier");
        String splType = toSPLType(valueClassName, typeModifier);
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
     * Generate a submission time value SPL param definition in an
     * inner (non-main) composite.
     * @param spval JSONObject for the submission parameter's value
     * @param sb
     */
    static void generateInnerDef(JSONObject spval, StringBuilder sb) {
        String name = "$__jaa_stv_" + (String) spval.get("name");
        String valueClassName = (String) spval.get("valueClassName");
        String typeModifier = (String) spval.get("typeModifier");
        String splType = toSPLType(valueClassName, typeModifier);
        Object defaultValue = spval.get("defaultValue");
        if (defaultValue == null)
            sb.append(String.format("expression<%s> %s;", splType, name));
        else {
            if (splType.startsWith("uint"))
                defaultValue = SPLGenerator.unsignedString(defaultValue);
            sb.append(String.format("expression<%s> %s : (%s) %s", splType, name, splType, defaultValue));
        }
    }
    
    /**
     * Generate a submission time value SPL param reference 
     * @param spval JSONObject for the submission parameter's value
     * @param sb
     */
    static void generateRef(JSONObject spval, StringBuilder sb) {
        String ref = "$__jaa_stv_" + (String) spval.get("name");
        sb.append(ref);
    }
    
    private static String toSPLType(String className, String modifier) {
        String splType = javaToSPL.get(className);
        if (splType == null)
            throw new IllegalArgumentException("Unhandled className "+className);
        if (modifier != null) {
            if ("ustring".equals(modifier))
                splType = "ustring";
            else if ("unsigned".equals(modifier))
                splType = "u" + splType;
        }
        return splType;
    }

}
