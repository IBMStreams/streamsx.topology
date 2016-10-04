/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SUBMISSION_PARAMETER;
import static com.ibm.streamsx.topology.generator.spl.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.generator.spl.GsonUtilities.jobject;
import static com.ibm.streamsx.topology.generator.spl.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.generator.spl.GsonUtilities.objectArray;
import static com.ibm.streamsx.topology.generator.spl.GsonUtilities.stringArray;
import static com.ibm.streamsx.topology.generator.spl.SPLGenerator.splBasename;
import static com.ibm.streamsx.topology.internal.functional.ops.SubmissionParameterManager.NAME_SUBMISSION_PARAM_NAMES;
import static com.ibm.streamsx.topology.internal.functional.ops.SubmissionParameterManager.NAME_SUBMISSION_PARAM_VALUES;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streams.operator.window.StreamWindow.Type;
import com.ibm.streamsx.topology.builder.JOperator;
import com.ibm.streamsx.topology.builder.JOperator.JOperatorConfig;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.generator.spl.SubmissionTimeValue.ParamsInfo;

class OperatorGenerator {
    
    private final SubmissionTimeValue stvHelper;
    
    OperatorGenerator(SPLGenerator splGenerator) {
        this.stvHelper = splGenerator.stvHelper();
    }

    String generate(JSONObject graphConfig, JSONObject op)
            throws IOException {
        JsonObject _op = GraphUtilities.gson(op);
        StringBuilder sb = new StringBuilder();
        noteAnnotations(_op, sb);
        parallelAnnotation(_op, sb);
        viewAnnotation(_op, sb);
        AutonomousRegions.autonomousAnnotation(_op, sb);
        outputClause(_op, sb);
        operatorNameAndKind(_op, sb);
        inputClause(_op, sb);

        sb.append("  {\n");
        windowClause(op, sb);
        paramClause(graphConfig, op, sb);
        configClause(graphConfig, op, sb);
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
            // removes the 'tuple' part of the type
            type = type.substring(5);

            String name = jstring(output, "name");
            name = splBasename(name);

            if (!first.get()) {
                sb.append("; ");              
            }
            first.set(false);

            sb.append("stream");
            sb.append(type);
            sb.append(" ");
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

    static JSONArray getInputs(JSONObject op) {
        JSONArray inputs = (JSONArray) op.get("inputs");
        if (inputs == null || inputs.isEmpty())
            return null;
        return inputs;

    }

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
                sb.append(name);
            });

            String name = jstring(input, "name");
            sb.append(" as ");
            sb.append(splBasename(name));
        });

        sb.append(")\n");
    }

    static void windowClause(JSONObject op, StringBuilder sb) {
        JSONArray inputs = getInputs(op);
        if (inputs == null) {
            return;
        }

        boolean seenWindow = false;
        for (int i = 0; i < inputs.size(); i++) {

            JSONObject input = (JSONObject) inputs.get(i);
            JSONObject window = (JSONObject) input.get("window");
            if (window == null)
                continue;
            String stype = (String) window.get("type");
            StreamWindow.Type type = StreamWindow.Type.valueOf(stype);
            if (type == Type.NOT_WINDOWED)
                continue;

            if (!seenWindow) {
                sb.append("  window\n");
                seenWindow = true;
            }

            sb.append("    ");
            sb.append(splBasename((String) input.get("name")));
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

            appendWindowPolicy(window.get("evictPolicy"),
                    window.get("evictConfig"), window.get("evictTimeUnit"), sb);

            Object triggerPolicy = window.get("triggerPolicy");
            if (triggerPolicy != null) {
                sb.append(", ");
                appendWindowPolicy(triggerPolicy, window.get("triggerConfig"), window.get("triggerTimeUnit"),
                        sb);
            }

            Boolean partitioned = (Boolean) window.get("partitioned");
            if (partitioned != null && partitioned) {
                sb.append(", partitioned");
            }
            sb.append(";\n");
        }

    }

    static void appendWindowPolicy(Object policyName, Object config, Object timeUnit,
            StringBuilder sb) {
        StreamWindow.Policy policy = StreamWindow.Policy
                .valueOf((String) policyName);
        switch (policy) {
        case COUNT:
            sb.append("count(");
            sb.append(config);
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
            Long time = (Long) config;
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

    private void paramClause(JSONObject graphConfig, JSONObject op,
            StringBuilder sb) {

        JSONArray vmArgs = (JSONArray) graphConfig
                .get(ContextProperties.VMARGS);
        boolean hasVMArgs = vmArgs != null && !vmArgs.isEmpty();

        // VMArgs only apply to Java SPL operators.
        boolean isJavaOp = JOperator.LANGUAGE_JAVA.equals(op.get(JOperator.LANGUAGE));
        hasVMArgs &= isJavaOp;

        // determine if we need to inject submission param names and values info. 
        boolean addSPInfo = false;
        ParamsInfo stvOpParamInfo = stvHelper.getSplInfo();
        if (stvOpParamInfo != null) {
            Map<String,JSONObject> functionalOps = stvHelper.getFunctionalOps();
            if (functionalOps.containsKey((String) op.get("name")))
                addSPInfo = true;
        }
        
        JSONObject params = (JSONObject) op.get("parameters");
        if (!hasVMArgs && (params == null || params.isEmpty())
            && !addSPInfo) {
            return;
        }

        sb.append("    param\n");

        for (Object on : params.keySet()) {
            String name = (String) on;
            JSONObject param = (JSONObject) params.get(name);
            if (hasVMArgs && "vmArg".equals(name)) {
                JSONArray ja = new JSONArray();
                ja.addAll(vmArgs);
                vmArgs = ja;
                Object value = param.get("value");
                if (value instanceof JSONArray)
                    vmArgs.addAll((JSONArray) value);
                else
                    vmArgs.add(value);
                continue;
            }
            sb.append("      ");
            sb.append(name);
            sb.append(": ");
            parameterValue(GraphUtilities.gson(param), sb);
            sb.append(";\n");
        }

        if (hasVMArgs) {
            JSONObject tmpVMArgParam = new JSONObject();
            tmpVMArgParam.put("value", vmArgs);
            sb.append("      ");
            sb.append("vmArg");
            sb.append(": ");

            parameterValue(GraphUtilities.gson(tmpVMArgParam), sb);
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

    static void configClause(JSONObject graphConfig, JSONObject op,
            StringBuilder sb) {
        if (!JOperator.hasConfig(op))
            return;

        boolean needsConfigSection = false;
        
        Boolean streamViewability = JOperatorConfig.getBooleanItem(op, "streamViewability");
        needsConfigSection = streamViewability != null;
        
        String colocationTag = null;
        String hostPool = null;
        boolean needsPlacement = false;
        JSONObject placement = JOperatorConfig.getJSONItem(op, JOperatorConfig.PLACEMENT);
        if (placement != null) {
            // Explicit placement takes precedence.
            colocationTag = (String) placement.get(JOperator.PLACEMENT_EXPLICIT_COLOCATE_ID);
            if (colocationTag == null)
                colocationTag = (String) placement.get(JOperator.PLACEMENT_ISOLATE_REGION_ID);
            
            if (colocationTag != null && colocationTag.isEmpty())
                colocationTag = null;
            
            Set<String> uniqueResourceTags = new HashSet<>();

            JSONArray resourceTags = (JSONArray) placement.get(JOperator.PLACEMENT_RESOURCE_TAGS);
            if (resourceTags != null && resourceTags.isEmpty())
                resourceTags = null;
            
            if (resourceTags != null) {
                for (Object rto : resourceTags) {
                    String rt = rto.toString();
                    if (!rt.isEmpty())
                        uniqueResourceTags.add(rt);                  
                }
            }
            
            needsPlacement = colocationTag != null || !uniqueResourceTags.isEmpty();
                      
            if (needsPlacement)
                needsConfigSection = true;
            
            if (!uniqueResourceTags.isEmpty()) {
                hostPool = getHostPoolName(graphConfig, uniqueResourceTags);         
            }
        }
        
        JSONObject queue = JOperatorConfig.getJSONItem(op, "queue");
        if (!needsConfigSection)
            needsConfigSection = queue != null && !queue.isEmpty();
        
        if (needsConfigSection) {
            sb.append("  config\n");
        }
        if (streamViewability != null) {
            sb.append("    streamViewability: ");
            sb.append(streamViewability);
            sb.append(";\n");
        }

        if (needsPlacement) {
            sb.append("    placement: \n");
        }
        if (colocationTag != null) {
            
            
            sb.append("      partitionColocation(");
            SPLGenerator.stringLiteral(sb, colocationTag);
            sb.append(")\n");
        }
        if (hostPool != null) {
            if (colocationTag != null)
                sb.append(",");
            sb.append("      host(");
            sb.append(hostPool);
            sb.append(")\n");
        }
        if (needsPlacement) {
            sb.append("    ;\n");
        }
        
        
        if(queue != null && !queue.isEmpty()){
            sb.append("    threadedPort: queue(");
            sb.append((String)queue.get("inputPortName") + ", ");
            sb.append((String)queue.get("congestionPolicy") + ",");
            sb.append(((Integer)queue.get("queueSize")).toString());
            sb.append(");\n");
        }
      
        
    }
    
    /**
     * Gets or creates a host pool at the graphConfig level
     * corresponding to the unique set of tags.
     */
    @SuppressWarnings("unchecked")
    private static String getHostPoolName(JSONObject graphConfig, Set<String> uniqueResourceTags) {
        String hostPool = null;
        JSONArray hostPools = (JSONArray) graphConfig.get("__spl_hostPools");
        if (hostPools == null) {
            graphConfig.put("__spl_hostPools", hostPools = new JSONArray());
        }
        
        // Look for a host pool matching this one
        for (Object hpo : hostPools) {
            JSONObject hostPoolDef = (JSONObject) hpo;
            JSONArray rta = (JSONArray) hostPoolDef.get("resourceTags");
            Set<Object> poolResourceTags = new HashSet<>();
            poolResourceTags.addAll(rta);
            if (uniqueResourceTags.equals(poolResourceTags)) {
                return hostPoolDef.get("name").toString();
            }
        }
                        
        JSONObject hostPoolDef = new JSONObject();
        hostPoolDef.put("name", hostPool = "__jaaHostPool" + hostPools.size());
        JSONArray rta = new JSONArray();
        rta.addAll(uniqueResourceTags);
        hostPoolDef.put("resourceTags", rta);
        hostPools.add(hostPoolDef);  
        return hostPool;
    }
}
