/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_COMPOSITE_PARAMETER;
import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SUBMISSION_PARAMETER;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_FUNCTIONAL;
import static com.ibm.streamsx.topology.internal.functional.FunctionalOpProperties.FUNCTIONAL_LOGIC_PARAM;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jobject;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.topology.builder.JParamTypes;
import com.ibm.streamsx.topology.generator.operator.OpProperties;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * A Submission Time Value is the SPL realization of a "Submission Parameter".
 */
public class SubmissionTimeValue {
    private static final String OP_ATTR_SPL_SUBMISSION_PARAMS = "__spl_submissionParams";
    
    /** map<name,opParam> opParam has type TYPE_SUBMISSION_PARAMETER */
    private final Map<String,JsonObject> allSubmissionParams = new HashMap<>();
    
    /**
     * Map of submission value names to the composite parameter
     * that represents them at the main composite level.
     */
    private final Map<String,JsonObject> submissionMainCompositeParams = new HashMap<>();
    
    
    /** map<opName,opJsonObject> */
    private Map<String,JsonObject> functionalOps = new HashMap<>();
    private ParamsInfo paramsInfo;
    
    /**
     * Info for SPL operator parameters
     * NAME_SUBMISSION_PARAM_NAMES and NAME_SUBMISSION_PARAM_VALUES
     */
    static class ParamsInfo {
        /** The value for the NAME_SUBMISSION_PARAM_NAMES param.
         * <p>
         * A string of comma separated literal STV opParamName strings.
         * e.g., "__jaa_threshold", "__jaa_width"
         */
        final String names;
        /** The value for the NAME_SUBMISSION_PARAM_VALUES param.
         * <p>
         * A string of comma separated STV opParam expression strings.
         * e.g., (rstring) $__jaa_threshold, (rstring) $__jaa_width 
         */
        final String values;
        
        ParamsInfo(String names, String values) {
            this.names = names;
            this.values = values;
        }
    }
    
    /**
     * Create a composite parameter name for the submission parameter name. 
     * @param spName the submission parameter name
     * @return the composite parameter name
     */
    private static String mkCompositeParamName(String spName) {
        spName = spName.replace('.', '_');
        return "__spl_stv_" + SPLGenerator.getSPLCompatibleName(spName);
    }

    SubmissionTimeValue(JsonObject graph) {
        createMainCompositeParamsForAllSubmissionValues(graph);
        paramsInfo = mkSubmissionParamsInfo();
    }
    
    /**
     * Get a collection of all of the submission parameters used in the graph.
     * @param graph
     * @return {@code map<spOpParamName,spParam>}
     */
    private void createMainCompositeParamsForAllSubmissionValues(JsonObject graph) {
        Map<String,JsonObject> all = this.allSubmissionParams;
        
        JsonObject params = GsonUtilities.jobject(graph, "parameters");
        
        if (params != null) {
            for (Entry<String, JsonElement> e : params.entrySet()) {
                JsonObject param = e.getValue().getAsJsonObject();
                if (TYPE_SUBMISSION_PARAMETER.equals(jstring(param, "type"))) {
                    JsonObject sp = jobject(param, "value");
                    all.put(jstring(sp, "name"), param);                  
                }
            }
        }
        
        for (JsonObject param : all.values())
            addMainCompositeParam(params, param);
    }
    
    /**
     * Create a main composite to represent a submission parameter.
     * 
     * A parameter with name width,  type int32 default 3 is mapped to in the main composite:
     * 
     * expression<int32> $__spl_stv_width : (int32) getSubmissionTimeValue("width", "3");
     * 
     */
    private void addMainCompositeParam(JsonObject params, JsonObject param) {
       
        JsonObject spv = jobject(param, "value");
        String spname = jstring(spv, "name");
        String metaType = jstring(spv, "metaType");
        String cpname  = mkCompositeParamName(spname);
        
        JsonObject cp = new JsonObject();
        cp.addProperty("type", TYPE_COMPOSITE_PARAMETER);
        
        JsonObject cpv = new JsonObject();
        cpv.addProperty("name", cpname);
        cpv.addProperty("metaType", metaType);
        
        String splspname = SPLGenerator.stringLiteral(spname);
        String splType = Types.metaTypeToSPL(metaType);
        
        String cpdv;
        if (!spv.has("defaultValue")) {
            cpdv = String.format("(%s) getSubmissionTimeValue(%s)", splType, splspname);
        } else {
            JsonPrimitive defaultValueJson = spv.get("defaultValue").getAsJsonPrimitive();
            String defaultValue;
            
            if (metaType.startsWith("UINT")) {
                StringBuilder sbunsigned = new StringBuilder();
                sbunsigned.append("(rstring) ");                
                SPLGenerator.numberLiteral(sbunsigned, defaultValueJson, metaType);
                defaultValue = sbunsigned.toString();
            } else {
                defaultValue = SPLGenerator.stringLiteral(defaultValueJson.getAsString());
            }         
            
            cpdv = String.format("(%s) getSubmissionTimeValue(%s, %s)", splType, splspname, defaultValue);
        }
        cpv.addProperty("defaultValue", cpdv);
        
        cp.add("value", cpv);
        
        params.add(cpname, cp);
        submissionMainCompositeParams.put(spname, cp);
    }
    
    /**
     * Create a inner composite to access a submission parameter defined in
     * a main composite..
     * 
     * A parameter with name width,  type int32 default 3 is mapped to in the main composite:
     * 
     * expression<int32> $__spl_stv_width;
     * 
     */
    private void addInnerCompositeParameter(JsonObject params, JsonObject param) {
        
        assert TYPE_SUBMISSION_PARAMETER.equals(jstring(param, "type"));
        
        JsonObject spv = jobject(param, "value");
        String spname = jstring(spv, "name");
        String metaType = jstring(spv, "metaType");
        String cpname  = mkCompositeParamName(spname);
        
        if (params.has(cpname))
            return;
              
        JsonObject cp = new JsonObject();
        cp.addProperty("type", TYPE_COMPOSITE_PARAMETER);
        
        JsonObject cpv = new JsonObject();
        cpv.addProperty("name", cpname);
        cpv.addProperty("metaType", metaType);
        
        cp.add("value", cpv);
        
        params.add(cpname, cp);       
    }
    
    /**
     * Create a ParamsInfo for the topology's submission parameters.
     * This is how submission parameter values are passed into
     * a functional operator.
     * @return the parameter info. null if no submission parameters.
     */
    private ParamsInfo mkSubmissionParamsInfo() {
        if (allSubmissionParams.isEmpty())
            return null;

        StringBuilder namesSb = new StringBuilder();
        StringBuilder valuesSb = new StringBuilder();
        
        boolean first = true;
        for (String opParamName : allSubmissionParams.keySet()) {
            JsonObject spParam = allSubmissionParams.get(opParamName);
            JsonObject spval = jobject(spParam, "value");
            String name = jstring(spval, "name");
            if (first)
                first = false;
            else {
                namesSb.append(", ");
                valuesSb.append(", ");
            }
            namesSb.append(SPLGenerator.stringLiteral(name));
            valuesSb.append("(rstring) ").append(generateCompositeParamReference(spval));
        }

        return new ParamsInfo(namesSb.toString(), valuesSb.toString());
    }
    
    /**
     * Enrich the json composite operator definition's parameters
     * to include parameters for submission parameters.
     * <p>
     * The composite is augmented with a TYPE_SUBMISSION_PARAMETER parameter
     * for each submission parameter used within the composite - e.g, as
     * a parallel width value or SPL operator parameter value.
     * <p>
     * If the composite has any functional operator children, enrich
     * the composite to have declarations for all submission parameters.
     * Also accumulate the functional children and make them available via
     * {@link #getFunctionalOps()}.
     * 
     * @param composite the composite definition
     */

    void addJsonParamDefs(JsonObject composite) {
        // scan immediate children ops for submission param use
        // and add corresponding param definitions to the composite.
        // Also, if the op has functional logic, enrich the op too...
        // and further enrich the composite.
        
        if (allSubmissionParams.isEmpty())
            return;
        
        // scan for spParams
        JsonObject spParams = new JsonObject();
        AtomicBoolean addedAll = new AtomicBoolean();
        GsonUtilities.objectArray(composite, "operators", op -> {
            
            boolean addAll = false;
            if (MODEL_FUNCTIONAL.equals(jstring(op, MODEL))) {
                functionalOps.put(jstring(op, "name"), op);
                addAll = true;
            } else {
                JsonObject params = jobject(op, "parameters");
                if (params != null) {
                    for (Entry<String, JsonElement> p : params.entrySet()) {
                        // if functional logic add "submissionParameters" param
                        JsonObject param = p.getValue().getAsJsonObject();
                        String type = jstring(param, "type");
                        if (TYPE_SUBMISSION_PARAMETER.equals(type)) {
                            addInnerCompositeParameter(spParams, param);
                        }
                    }
                }
            }
            if (addAll && !addedAll.getAndSet(true)) {
                for (String name : allSubmissionParams.keySet()) {
                    addInnerCompositeParameter(spParams, allSubmissionParams.get(name));
                }
            }
            boolean isParallel = jboolean(op, "parallelOperator"); 
            if (isParallel) {
                JsonElement width = op.get("parallelInfo").getAsJsonObject().get("width");
                if (width.isJsonObject()) {
                    JsonObject jwidth = width.getAsJsonObject(); 
                    String type = jstring(jwidth, "type");
                    if (TYPE_SUBMISSION_PARAMETER.equals(type)) {
                        addInnerCompositeParameter(spParams, jwidth);
                    }
                }
            }
        });
        
        // augment the composite's parameters
        JsonObject params = jobject(composite, "parameters");
        if (params == null && !GsonUtilities.jisEmpty(spParams)) {
            params = new JsonObject();
            composite.add("parameters", params);
        }
        for (Entry<String, JsonElement> p : spParams.entrySet()) {
            String pname = p.getKey();
            if (!params.has(pname))
                params.add(pname, spParams.get(pname));
        }
        
        // make the results of our efforts available to addJsonInstanceParams
        composite.add(OP_ATTR_SPL_SUBMISSION_PARAMS, spParams);
    }

    /**
     * Akin to addJsonParamDefs(), enrich the json composite operator instance's
     * invocation parameters with submission parameter references.
     * @param compInstance the composite instance
     * @param composite the composite definition
     */
    void addJsonInstanceParams(JsonObject compInstance, JsonObject composite) {
        JsonObject spParams = jobject(composite, OP_ATTR_SPL_SUBMISSION_PARAMS);
        if (spParams != null) {
            JsonObject opParams = jobject(compInstance, "parameters");
            if (opParams == null) {
                compInstance.add("parameters", opParams = new JsonObject());
            }
            for (Entry<String, JsonElement> p : spParams.entrySet()) {
                JsonObject spParam = p.getValue().getAsJsonObject();
                
                // need to end up generating: __spl_stv_foo : $__spl_stv_foo;
                opParams.add(p.getKey(), compositeParameterReference(spParam));
            }
        }
    }
    
    /**
     * Return a SPL expression that accesses
     * a submission time value.
     * @param name
     * @return
     */
    JsonObject getSPLExpression(JsonObject param) {
        assert jstring(param, "type").equals(TYPE_SUBMISSION_PARAMETER);
        
        String name = jstring(jobject(param, "value"), "name");
        
        return compositeParameterReference(submissionMainCompositeParams.get(name));
    }
    
    /**
     * Create an SPL expression that is a reference to a
     * composite parameter.
     */
    private static JsonObject compositeParameterReference(JsonObject compParam) {
        
        assert jstring(compParam, "type").equals(TYPE_COMPOSITE_PARAMETER);
       
        JsonObject spval = jobject(compParam, "value");
        String name = jstring(spval, "name");
        
        JsonObject ref = new JsonObject();
        ref.addProperty("type", JParamTypes.TYPE_SPL_EXPRESSION);
        ref.addProperty("value", "$" + name);
        return ref;
    }
    
//    /** Get the graph's submission parameters in the form of a
//     * operator parameter of TYPE_SPL_SUBMISSION_PARAMS
//     * @return the op parameter. null if no submission params in the topology.
//     */
//    JSONObject getSubmissionParamsParam() {
//        return submissionParamsParam;
//    }
    
    /** Get the info for operator NAME_SUBISSION_PARAM_NAMES
     * and NAMEgraph's submission parameter info in the form of a
     * operator parameter of TYPE_SPL_SUBMISSION_PARAMS
     * @return the op parameter. null if no submission params in the topology.
     */
    ParamsInfo getSplInfo() {
        return paramsInfo;
    }
    
    /** Get the list of functional ops learned by {@link #addJsonParamDefs(JSONObject)}.
     * @return the collection of functional ops map<opName, opJsonObject>
     */
    Map<String,JsonObject> getFunctionalOps() {
        return functionalOps;
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
    void generateMainDef(JsonObject spval, StringBuilder sb) {
        String paramName = generateCompositeParamReference(spval);
        String spName = SPLGenerator.stringLiteral(jstring(spval, "name"));
        String metaType = jstring(spval, "metaType");
        String splType = Types.metaTypeToSPL(metaType);
        
        sb.append(String.format("expression<%s> %s : ", splType, paramName));
        if (!spval.has("defaultValue")) {
            sb.append(String.format("(%s) getSubmissionTimeValue(%s)", splType, spName));
        } else {
            JsonPrimitive defaultValueJson = spval.get("defaultValue").getAsJsonPrimitive();
            String defaultValue;
            
            if (metaType.startsWith("UINT")) {
                StringBuilder sbunsigned = new StringBuilder();
                sbunsigned.append("(rstring) ");                
                SPLGenerator.numberLiteral(sbunsigned, defaultValueJson, metaType);
                defaultValue = sbunsigned.toString();
            } else {
                defaultValue = SPLGenerator.stringLiteral(defaultValueJson.getAsString());
            }         
            
            sb.append(String.format("(%s) getSubmissionTimeValue(%s, %s)", splType, spName, defaultValue));
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
    void generateInnerDef(JsonObject spval, StringBuilder sb) {
        String paramName = generateCompositeParamReference(spval);
        String metaType = jstring(spval, "metaType");
        String splType = Types.metaTypeToSPL(metaType);
        sb.append(String.format("expression<%s> %s", splType, paramName));
    }
    
    /**
     * Generate a $__jaa_stv_... composite parameter name
     * for the submission time value parameter. 
     * <p>
     * <pre><code>
     *  composite {
     *  param expression<int32> $__jaa_stv_foo = (int32) getSubmissionTimeValue(...)
     *  graph      
     *     {@literal @}parallel(width=$__jaa_stv_foo)
     * </code></pre>
     * @param spval JSONObject for the submission parameter's value
     * @return the name
     */
    static String generateCompositeParamReference(JsonObject spval) {
        return "$" + mkCompositeParamName(spval.get("name").getAsString());
    }

}
