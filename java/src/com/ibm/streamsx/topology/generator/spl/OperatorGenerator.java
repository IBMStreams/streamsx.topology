/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SUBMISSION_PARAMETER;
import static com.ibm.streamsx.topology.generator.spl.SPLGenerator.splBasename;
import static com.ibm.streamsx.topology.internal.functional.ops.SubmissionParameterManager.NAME_SUBMISSION_PARAM_NAMES;
import static com.ibm.streamsx.topology.internal.functional.ops.SubmissionParameterManager.NAME_SUBMISSION_PARAM_VALUES;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jobject;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectArray;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.stringArray;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streams.operator.window.StreamWindow.Type;
import com.ibm.streamsx.topology.builder.JOperator;
import com.ibm.streamsx.topology.builder.JOperator.JOperatorConfig;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.generator.spl.SubmissionTimeValue.ParamsInfo;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

class OperatorGenerator {
    
    private final SubmissionTimeValue stvHelper;
    
    OperatorGenerator(SPLGenerator splGenerator) {
        this.stvHelper = splGenerator.stvHelper();
    }

    String generate(JsonObject graphConfig, JsonObject op)
            throws IOException {
        JsonObject _op = op;
        StringBuilder sb = new StringBuilder();
        noteAnnotations(_op, sb);
        parallelAnnotation(_op, sb);
        viewAnnotation(_op, sb);
        AutonomousRegions.autonomousAnnotation(_op, sb);
        outputClause(_op, sb);
        operatorNameAndKind(_op, sb);
        inputClause(_op, sb);

        sb.append("  {\n");
        windowClause(_op, sb);
        paramClause(graphConfig, _op, sb);
        configClause(graphConfig, _op, sb);
        sb.append("  }\n");

        return sb.toString();
    }

    private static void noteAnnotations(JsonObject op, StringBuilder sb)
            throws IOException {
        
        sourceLocationNote(op, sb);
        portTypesNote(op, sb);
    }
    
    private static void sourceLocationNote(JsonObject op, StringBuilder sb) throws IOException {
        
        JsonArray ja = GsonUtilities.array(op, "sourcelocation");
        if (ja == null)
            return;

        JsonElement jsource = ja.size() == 1 ? (JsonElement) ja.get(0) : ja;

        sb.append("@spl_note(id=\"__spl_sourcelocation\"");
        sb.append(", text=");
        String sourceInfo = jsource.toString();
        SPLGenerator.stringLiteral(sb, sourceInfo);
        sb.append(")\n");
    }
    
    private static void portTypesNote(JsonObject op, StringBuilder sb) {
        
        int[] id = new int[1];
        GsonUtilities.objectArray(op, "outputs",
                output -> {
                    
            String type = GsonUtilities.jstring(output, "type.native");
            if (type == null || type.isEmpty())
                return;
            sb.append("@spl_note(id=\"__spl_nativeType_output_" + id[0]++ + "\"");
            sb.append(", text=");
            SPLGenerator.stringLiteral(sb, type);
            sb.append(")\n");
        });
    }

    private void viewAnnotation(JsonObject op, StringBuilder sb) {
        
        JsonObject config = jobject(op, "config");
        if (config == null)
            return;
        
        objectArray(config, "viewConfigs", viewConfig -> {

            String name = viewConfig.get("name").getAsString();
            String port = viewConfig.get("port").getAsString();
            Double bufferTime = viewConfig.get("bufferTime").getAsDouble();
            Long sampleSize = viewConfig.get("sampleSize").getAsLong();
            sb.append("@view(name = \"");
            sb.append(splBasename(name));
            sb.append("\", port = " + port);
            sb.append(", bufferTime = " + bufferTime + ", ");
            sb.append("sampleSize = " + sampleSize + ", ");
            sb.append("activateOption = firstAccess)\n");
        });
    }

    private void parallelAnnotation(JsonObject op, StringBuilder sb) {
        boolean parallel = jboolean(op, "parallelOperator");
        
        if (parallel) {
            sb.append("@parallel(width=");
            JsonElement width = op.get("width");
            if (width.isJsonPrimitive()) {
                sb.append(width.getAsString());
            }
            else {
                JsonObject jo = width.getAsJsonObject();
                String jsonType = jo.get("type").getAsString();
                if (TYPE_SUBMISSION_PARAMETER.equals(jsonType))
                    sb.append(SubmissionTimeValue.generateCompParamName(jo.get("value").getAsJsonObject()));
                else
                    throw new IllegalArgumentException("Unsupported parallel width specification: " + jo);
            }
            boolean partitioned = jboolean(op, "partitioned");
            if (partitioned) {
                String parallelInputPortName = op.get("parallelInputPortName").getAsString();
                parallelInputPortName = splBasename(parallelInputPortName);
                sb.append(", partitionBy=[{port=" + parallelInputPortName
                        + ", attributes=[__spl_hash]}]");
            }
            sb.append(")\n");
        }
    }

    /**
     * Create the output port definitions.
     */
    static void outputClause(JsonObject op, StringBuilder sb) {

        sb.append("  ( ");
        
        // effectively a mutable boolean
        AtomicBoolean first = new AtomicBoolean(true);
        
        objectArray(op, "outputs", output -> {

            String type = jstring(output, "type");
            if (type.startsWith("tuple<")) {
                // removes the 'tuple<..>' part of the type
                type = type.substring(6, type.length()-1);
            }

            String name = jstring(output, "name");
            name = splBasename(name);

            if (!first.get()) {
                sb.append("; ");              
            }
            first.set(false);

            sb.append("stream<");
            sb.append(type);
            sb.append("> ");
            sb.append(name);
        });

        sb.append(") ");
    }

    static void operatorNameAndKind(JsonObject op, StringBuilder sb) {
        String name = jstring(op, "name");
        name = splBasename(name);

        sb.append("as ");
        sb.append(name);
        // sb.append("_op");
        /*
         * JSONArray outputs = (JSONArray) op.get("outputs"); if (outputs ==
         * null || outputs.isEmpty()) { sb.append("_sink"); }
         */

        String kind = jstring(op, "kind");
        sb.append(" = ");
        sb.append(kind);
    }

    /*
    static JSONArray getInputs(JSONObject op) {
        JSONArray inputs = (JSONArray) op.get("inputs");
        if (inputs == null || inputs.isEmpty())
            return null;
        return inputs;

    }
    */

    static void inputClause(JsonObject op, StringBuilder sb) {

        sb.append("  ( ");
        
        AtomicBoolean firstPort = new AtomicBoolean(true);
        
        objectArray(op, "inputs", input ->  {
            
            if (!firstPort.getAndSet(false))
                sb.append("; ");
            
            AtomicBoolean firstStream = new AtomicBoolean(true);
            stringArray(input, "connections", name -> {
                if (!firstStream.getAndSet(false))
                    sb.append(", ");
                sb.append(splBasename(name));
            });

            String name = jstring(input, "name");
            sb.append(" as ");
            sb.append(splBasename(name));
        });

        sb.append(")\n");
    }

    static void windowClause(JsonObject op, StringBuilder sb) {

        AtomicBoolean firstWindow = new AtomicBoolean(true);
        
        objectArray(op, "inputs", input ->  {
            
            JsonObject window = jobject(input, "window");
            if (window == null)
                return;
            
            String stype = jstring(window, "type");
            StreamWindow.Type type = StreamWindow.Type.valueOf(stype);
            if (type == Type.NOT_WINDOWED)
                return;

            if (firstWindow.getAndSet(false))
                sb.append("  window\n");

            sb.append("    ");
            sb.append(splBasename(jstring(input, "name")));
            sb.append(":");
            switch (type) {
            case SLIDING:
                sb.append("sliding,");
                break;
            case TUMBLING:
                sb.append("tumbing,");
                break;
            default:
                throw new IllegalStateException("Internal error");
            }

            appendWindowPolicy(jstring(window, "evictPolicy"),
                    window.get("evictConfig"), jstring(window, "evictTimeUnit"), sb);

            String triggerPolicy = jstring(window, "triggerPolicy");
            if (triggerPolicy != null) {
                sb.append(", ");
                appendWindowPolicy(triggerPolicy, window.get("triggerConfig"), jstring(window, "triggerTimeUnit"),
                        sb);
            }

            if (jboolean(window, "partitioned"))
                sb.append(", partitioned");

            sb.append(";\n");
        });

    }

    static void appendWindowPolicy(String policyName, JsonElement config, String timeUnit,
            StringBuilder sb) {
        StreamWindow.Policy policy = StreamWindow.Policy
                .valueOf((String) policyName);
        switch (policy) {
        case COUNT:
            sb.append("count(");
            sb.append(config.getAsInt());
            sb.append(")");
            break;
        case DELTA:
            break;
        case NONE:
            break;
        case PUNCTUATION:
            break;
        case TIME:
        {
            TimeUnit unit = TimeUnit.valueOf(timeUnit.toString());
            long time = config.getAsLong();
            double secs;
            switch (unit) {
            case DAYS:
            case HOURS:
            case MINUTES:
            case SECONDS:
                secs = unit.toSeconds(time);
                break;
            case MILLISECONDS:
                secs = ((double) time) / 1000.0;
                break;
                
            case MICROSECONDS:
                secs = ((double) time) / 1000_000.0;
                break;

            case NANOSECONDS:
                secs = ((double) time) / 1000_000_000.0;
                break;

            default:
                throw new IllegalStateException();
            }
            sb.append("time(");
            sb.append(secs);
            sb.append(")");
            break;
        }
        default:
            break;

        }
    }

    private void paramClause(JsonObject graphConfig, JsonObject op,
            StringBuilder sb) {
        

        // VMArgs only apply to Java SPL operators.
        boolean isJavaOp = JOperator.LANGUAGE_JAVA.equals(jstring(op, JOperator.LANGUAGE));

        JsonArray vmArgs = null;
        if (isJavaOp && graphConfig.has(ContextProperties.VMARGS))
            vmArgs = GsonUtilities.array(graphConfig, ContextProperties.VMARGS);

        // determine if we need to inject submission param names and values info. 
        boolean addSPInfo = false;
        ParamsInfo stvOpParamInfo = stvHelper.getSplInfo();
        if (stvOpParamInfo != null) {
            Map<String,JsonObject> functionalOps = stvHelper.getFunctionalOps();
            if (functionalOps.containsKey(op.get("name").getAsString()))
                addSPInfo = true;
        }
        
        JsonObject params = jobject(op, "parameters");
        if (vmArgs == null && GsonUtilities.jisEmpty(params)
            && !addSPInfo) {
            return;
        }

        sb.append("    param\n");       
        
        for (Entry<String, JsonElement> on : params.entrySet()) {
            String name = on.getKey();
            JsonObject param = on.getValue().getAsJsonObject();
            if ("vmArg".equals(name)) {
                JsonArray fullVmArgs = new JsonArray();
                fullVmArgs.addAll(GsonUtilities.array(param, "value"));
                if (vmArgs != null)
                    fullVmArgs.addAll(vmArgs);
                //stringArray(param, "value", v -> fullVmArgs.);
                // objectArray(graphConfig, ContextProperties.VMARGS, v -> fullVmArgs.add(v));  
                vmArgs = fullVmArgs;
                continue;
            }
            sb.append("      ");
            sb.append(name);
            sb.append(": ");
            parameterValue(param, sb);
            sb.append(";\n");
        }

        if (vmArgs != null) {
            JsonObject tmpVMArgParam = new JsonObject();
            tmpVMArgParam.add("value", vmArgs);
            System.err.println("TEMP:" + tmpVMArgParam);
            sb.append("      ");
            sb.append("vmArg");
            sb.append(": ");

            parameterValue(tmpVMArgParam, sb);
            sb.append(";\n");
        }
        
        if (addSPInfo) {
            sb.append("      ");
            sb.append(NAME_SUBMISSION_PARAM_NAMES);
            sb.append(": ");
            sb.append(stvOpParamInfo.names);
            sb.append(";\n");

            sb.append("      ");
            sb.append(NAME_SUBMISSION_PARAM_VALUES);
            sb.append(": ");
            sb.append(stvOpParamInfo.values);
            sb.append(";\n");
        }
    }

    private void parameterValue(JsonObject param, StringBuilder sb) {
        JsonElement value = param.get("value");
        JsonElement type = param.get("type");
        if (param.has("type") && TYPE_SUBMISSION_PARAMETER.equals(type.getAsString())) {
            sb.append(stvHelper.generateCompParamName(value.getAsJsonObject()));
            return;
        }
        
        SPLGenerator.value(sb, param);
    }

    static void configClause(JsonObject graphConfig, JsonObject op,
            StringBuilder sb) {
        
        if (!op.has(JOperator.CONFIG))
            return;
        
        JsonObject config = jobject(op, JOperator.CONFIG);
        
        StringBuilder sbConfig = new StringBuilder();
        
        if (config.has("streamViewability")) {
            sbConfig.append("    streamViewability: ");
            sbConfig.append(jboolean(config, "streamViewability"));
            sbConfig.append(";\n");
        }
        
        if (config.has("queue")) {
            JsonObject queue = jobject(config, "queue");
            if (!queue.entrySet().isEmpty()) {
                sbConfig.append("    threadedPort: queue(");
                sbConfig.append(jstring(queue, "inputPortName") + ", ");
                sbConfig.append(jstring(queue, "congestionPolicy") + ",");
                sbConfig.append(jstring(queue, "queueSize"));
                sbConfig.append(");\n");
            }
        }
               
        if (config.has(JOperatorConfig.PLACEMENT)) {
            JsonObject placement = jobject(config, JOperatorConfig.PLACEMENT);
            StringBuilder sbPlacement = new StringBuilder();
            
            // Explicit placement takes precedence.
            String colocationTag = jstring(placement, JOperator.PLACEMENT_EXPLICIT_COLOCATE_ID);
            if (colocationTag == null)
                colocationTag = jstring(placement, JOperator.PLACEMENT_ISOLATE_REGION_ID);
            
            if (colocationTag != null && !colocationTag.isEmpty()) {
                sbPlacement.append("      partitionColocation(");
                SPLGenerator.stringLiteral(sbPlacement, colocationTag);
                sbPlacement.append(")\n");
            }
            
            Set<String> uniqueResourceTags = new HashSet<>();           
            GsonUtilities.stringArray(placement, JOperator.PLACEMENT_RESOURCE_TAGS, tag -> {if (!tag.isEmpty()) uniqueResourceTags.add(tag);} );
            if (!uniqueResourceTags.isEmpty()) {
                String hostPool = getHostPoolName(graphConfig, uniqueResourceTags);
                if (sbPlacement.length() != 0)
                    sbPlacement.append(",");
                sbPlacement.append("      host(");
                sbPlacement.append(hostPool);
                sbPlacement.append(")\n");
            }
            
            if (sbPlacement.length() != 0) {
                sbConfig.append("   placement: ");
                sbConfig.append(sbPlacement);
                sbConfig.append("    ;\n");
            }
        }
                
        if (sbConfig.length() != 0) {
            sb.append("  config\n");
            sb.append(sbConfig);
        }
    }
    
    /**
     * Gets or creates a host pool at the graphConfig level
     * corresponding to the unique set of tags.
     */
    private static String getHostPoolName(JsonObject graphConfig, Set<String> uniqueResourceTags) {
        JsonArray hostPools = array(graphConfig, "__spl_hostPools");
        if (hostPools == null) {
            graphConfig.add("__spl_hostPools", hostPools = new JsonArray());
        }
        
        // Look for a host pool matching this one
        for (JsonElement hpe : hostPools) {
            JsonObject hostPoolDef = hpe.getAsJsonObject();
            JsonArray rta = hostPoolDef.get("resourceTags").getAsJsonArray();
            Set<String> poolResourceTags = new HashSet<>();
            for (JsonElement tage : rta)
                poolResourceTags.add(tage.getAsString());
            if (uniqueResourceTags.equals(poolResourceTags)) {
                return jstring(hostPoolDef, "name");
            }
        }
                        
        JsonObject hostPoolDef = new JsonObject();
        String hostPool;
        hostPoolDef.addProperty("name", hostPool = "__jaaHostPool" + hostPools.size());
        JsonArray rta = new JsonArray();
        for (String tag : uniqueResourceTags)
            rta.add(new JsonPrimitive(tag));
        hostPoolDef.add("resourceTags", rta);
        hostPools.add(hostPoolDef);  
        return hostPool;
    }
}
