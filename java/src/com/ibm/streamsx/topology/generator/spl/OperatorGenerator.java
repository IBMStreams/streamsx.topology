/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SUBMISSION_PARAMETER;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.CONSISTENT;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT;
import static com.ibm.streamsx.topology.generator.operator.WindowProperties.POLICY_COUNT;
import static com.ibm.streamsx.topology.generator.operator.WindowProperties.POLICY_DELTA;
import static com.ibm.streamsx.topology.generator.operator.WindowProperties.POLICY_NONE;
import static com.ibm.streamsx.topology.generator.operator.WindowProperties.POLICY_PUNCTUATION;
import static com.ibm.streamsx.topology.generator.operator.WindowProperties.POLICY_TIME;
import static com.ibm.streamsx.topology.generator.operator.WindowProperties.TYPE_NOT_WINDOWED;
import static com.ibm.streamsx.topology.generator.operator.WindowProperties.TYPE_SLIDING;
import static com.ibm.streamsx.topology.generator.operator.WindowProperties.TYPE_TUMBLING;
import static com.ibm.streamsx.topology.generator.port.PortProperties.inputPortRef;
import static com.ibm.streamsx.topology.generator.spl.SPLGenerator.getSPLCompatibleName;
import static com.ibm.streamsx.topology.generator.spl.SPLGenerator.stringLiteral;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_COLOCATE_IDS;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_COLOCATE_TAG_MAPPING;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jobject;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectArray;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectCreate;
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
import com.ibm.streamsx.topology.builder.JParamTypes;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.generator.operator.OpProperties;
import com.ibm.streamsx.topology.generator.spl.SubmissionTimeValue.ParamsInfo;
import com.ibm.streamsx.topology.internal.functional.FunctionalOpProperties;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.messages.Messages;
import com.ibm.streamsx.topology.spi.builder.SourceInfo;

class OperatorGenerator {

    private final SubmissionTimeValue stvHelper;
    private final SPLGenerator splGenerator;

    OperatorGenerator(SPLGenerator splGenerator) {
        this.splGenerator = splGenerator;
        this.stvHelper = splGenerator.stvHelper();
    }

    String generate(JsonObject graphConfig, JsonObject op) throws IOException {
        JsonObject _op = op;
        StringBuilder sb = new StringBuilder();
        noteAnnotations(_op, sb);
        categoryAnnotation(_op, sb);
        parallelAnnotation(_op, sb);
        lowLatencyAnnotation(_op, sb);
        viewAnnotation(_op, sb);
        consistentAnnotation(_op, sb);
        AutonomousRegions.autonomousAnnotation(_op, sb);
        threadingAnnotation(graphConfig, _op, sb);
        boolean singlePortSingleName = outputPortClause(_op, sb);
        operatorNameAndKind(_op, sb, singlePortSingleName);
        inputClause(_op, sb);

        sb.append("  {\n");
        windowClause(_op, sb);
        paramClause(graphConfig, _op, sb);
        outputAssignmentClause(graphConfig, _op, sb);
        configClause(graphConfig, _op, sb);
        sb.append("  }\n");

        return sb.toString();
    }

    private static void noteAnnotations(JsonObject op, StringBuilder sb) throws IOException {

        layoutNote(op, sb);
        sourceLocationNote(op, sb);
        portTypesNote(op, sb);
    }

    private static void layoutNote(JsonObject op, StringBuilder sb) {
        final JsonObject layout = object(op, "layout");
        if (layout != null) {
            
            // Need to check for name mangling from provided names to SPL identifiers
            layoutMapName(layout, jstring(op, "name"));
            
            if (op.has("outputs")) {
                JsonArray outputs = array(op, "outputs");
                for (int i = 0; i < outputs.size(); i++) {
                    JsonObject output = outputs.get(i).getAsJsonObject();
                    String name = jstring(output, "name");
                    layoutMapName(layout, name);
                }
            }

            if (op.has("inputs")) {
                JsonArray inputs = array(op, "inputs");
                for (int i = 0; i < inputs.size(); i++) {
                    JsonObject input = inputs.get(i).getAsJsonObject();
                    String alias = jstring(input, "alias");
                    if (alias != null)
                        layoutMapName(layout, alias);
                    else {
                        JsonArray conns = array(input, "connections");
                        if (conns != null && conns.size() > 0) {
                            String portName = conns.get(0).getAsString();
                            layoutMapName(layout, portName  );
                        }
                    }
                }
            }

            appendNoteAnnotation(sb, "__spl_layout", layout);
        }
    }
    
    /**
     * Add a name mapping from the original name to the SPL identifier.
     * Three cases: 
     *   1) Name is valid as an SPL identifier, mapping left unchanged (if it exists or not).
     *   2) Name is original and no mapping exists: Mapping of {id:name} added.
     *   3) Name is not original and mapping is updated, e.g:
     *          on entry: {name:original}
     *          on exit:  {id:original}
     */
    private static void layoutMapName(final JsonObject layout, final String name) {
        final String id = getSPLCompatibleName(name);
        if (id == name) // yes - reference comparison
            return;
        
        JsonObject names = objectCreate(layout, "names");
        if (names.has(name)) {
            JsonElement origName = names.remove(name);
            names.add(id, origName);
        } else {
            names.addProperty(id, name);
        }
    }

    private static void sourceLocationNote(JsonObject op, StringBuilder sb) throws IOException {

        JsonArray ja = array(op, SourceInfo.SOURCE_LOCATIONS);
        if (ja == null)
            return;

        JsonElement jsource = ja.size() == 1 ? (JsonElement) ja.get(0) : ja;

        appendNoteAnnotation(sb, "__spl_sourcelocation", jsource);
    }

    private static void appendNoteAnnotation(StringBuilder sb, String type, Object textObject) {
        sb.append("@spl_note(id=\"");
        sb.append(type);
        sb.append("\"");
        sb.append(", text=");
        SPLGenerator.stringLiteral(sb, textObject.toString());
        sb.append(")\n");
    }

    private static void categoryAnnotation(JsonObject op, StringBuilder sb) {
        String category = jstring(op, "category");
        if (category != null) {
            sb.append("@spl_category(name=");
            SPLGenerator.stringLiteral(sb, category);
            sb.append(")\n");
        }
    }

    private static void portTypesNote(JsonObject op, StringBuilder sb) {

        int[] id = new int[1];
        GsonUtilities.objectArray(op, "outputs", output -> {

            String type = GsonUtilities.jstring(output, "type.native");
            if (type == null || type.isEmpty())
                return;
            appendNoteAnnotation(sb, "__spl_nativeType_output_" + id[0]++, type);
        });
    }

    private void viewAnnotation(JsonObject op, StringBuilder sb) {

        JsonObject config = jobject(op, "config");
        if (config == null)
            return;

        objectArray(config, "viewConfigs", viewConfig -> {

            String name = jstring(viewConfig, "name");
            String port = jstring(viewConfig, "port");
            String description = jstring(viewConfig, "description");
            Double bufferTime = viewConfig.get("bufferTime").getAsDouble();
            Long sampleSize = viewConfig.get("sampleSize").getAsLong();
            String activate = jstring(viewConfig, "activateOption");
            sb.append("@view(name = \"" + name + "\"");
            if (description != null) {
                sb.append(", description = \"" + description + "\"");
            }
            sb.append(", port = " + port);
            sb.append(", bufferTime = " + bufferTime);
            sb.append(", sampleSize = " + sampleSize);
            if (activate != null)
                sb.append(", activateOption = " + activate);
            sb.append(")\n");
        });
    }

    private void lowLatencyAnnotation(JsonObject op, StringBuilder sb){
        boolean lowLatencyOperator = jboolean(op, "lowLatency");
        if(lowLatencyOperator){
            sb.append("@threading(model=manual)\n");
        }
    }
    
    private void parallelAnnotation(JsonObject op, StringBuilder sb) {
        boolean parallel = jboolean(op, "parallelOperator");
        
        if (parallel) {
            boolean partitioned = jboolean(op, "partitioned");
            JsonObject parallelInfo = op.get("parallelInfo").getAsJsonObject();
            
            sb.append("@parallel(width=");
            JsonElement width = parallelInfo.get(OpProperties.WIDTH);
            if (width.isJsonPrimitive()) {
                sb.append(width.getAsString());
            } else {
                splValueSupportingSubmission(width.getAsJsonObject(), sb);
            }
      
            if (partitioned) {
                sb.append(", partitionBy=[");
                JsonArray partitionedPorts = array(parallelInfo, "partitionedPorts");
                for(int i = 0; i < partitionedPorts.size(); i++){
                    JsonObject partitionedPort = partitionedPorts.get(i).getAsJsonObject();
                    
                    if(i>0)
                        sb.append(", ");
                    
                    sb.append("{port=");
                    sb.append(getSPLCompatibleName(GsonUtilities.jstring(partitionedPort, "name")));
                    sb.append(", attributes=[");
                    JsonArray partitionKeys = partitionedPort.get("partitionedKeys").getAsJsonArray();
                    for (int j = 0; j < partitionKeys.size(); j++) {
                        if (j != 0)
                            sb.append(", ");
                        sb.append(partitionKeys.get(j).getAsString());
                    }
                    sb.append("]}");
                }
                
                sb.append("]"); 
            }
            
            JsonArray broadcastPorts = parallelInfo.get("broadcastPorts").getAsJsonArray();
            if(broadcastPorts.size() > 0){
                sb.append(", broadcast=[");
                for(int i = 0; i < broadcastPorts.size(); i++){
                    if (i != 0)
                        sb.append(", ");
                    sb.append(getSPLCompatibleName(broadcastPorts.get(i).getAsString()));
                }
                sb.append("]");
                
            }
            
       
            sb.append(")");
            sb.append("\n");
        }
    }

    /**
     * Add threading annotation but only for 4.2 onwards.
     */
    private void threadingAnnotation(JsonObject graphConfig, JsonObject op, StringBuilder sb) {
        if (!splGenerator.versionAtLeast(4, 2))
            return;

        JsonObject threading = object(op, "threading");
        if (threading != null) {
            sb.append("@threading(");
            sb.append("model=");
            sb.append(jstring(threading, "model"));
            sb.append(")\n");
        }
    }
    private static void asFloat64(StringBuilder sb, JsonObject obj, String key) {
        SPLGenerator.numberLiteral(sb, obj.getAsJsonPrimitive(key), "FLOAT64");
    }
    
    private void consistentAnnotation(JsonObject op, StringBuilder sb) {

        JsonObject consistent = object(op, CONSISTENT);
        if (consistent != null) {
            sb.append("@consistent(");
            if (consistent.has("period")) {
                sb.append("trigger=periodic,period=");
                asFloat64(sb, consistent, "period");
                sb.append(",");
            } else {
                sb.append("trigger=operatorDriven,");
            }
            sb.append("drainTimeout=");
            asFloat64(sb, consistent, "drainTimeout");
            sb.append(",");
            
            sb.append("resetTimeout=");
            asFloat64(sb, consistent, "resetTimeout");
            sb.append(",");
            
            sb.append("maxConsecutiveResetAttempts=");
            asFloat64(sb, consistent, "maxConsecutiveResetAttempts");
            
            sb.append(")\n");
        }
    }

    /**
     * Create the output port definitions.
     */
    private static boolean outputPortClause(JsonObject op, StringBuilder sb) {

        boolean singlePortSingleName = false;
        if (op.has("outputs")) {
            JsonArray outputs = array(op, "outputs");
            if (outputs.size() == 1) {
                JsonObject output = outputs.get(0).getAsJsonObject();
                String name = jstring(output, "name");

                if (name.equals(jstring(op, "name")))
                    singlePortSingleName = true;
            }
        }

        if (!singlePortSingleName)
            sb.append("  ( ");

        // effectively a mutable boolean
        AtomicBoolean first = new AtomicBoolean(true);

        objectArray(op, "outputs", output -> {

            String type = jstring(output, "type");
            if (type.startsWith("tuple<")) {
                // removes the 'tuple<..>' part of the type
                type = type.substring(6, type.length() - 1);
            }

            String name = jstring(output, "name");
            name = getSPLCompatibleName(name);

            if (!first.get()) {
                sb.append("; ");
            }
            first.set(false);

            sb.append("stream<");
            sb.append(type);
            sb.append("> ");
            sb.append(name);
        });

        if (!singlePortSingleName)
            sb.append(") ");

        return singlePortSingleName;
    }

    static void operatorNameAndKind(JsonObject op, StringBuilder sb, boolean singlePortSingleName) {
        if (!singlePortSingleName) {
            String name = jstring(op, "name");
            name = getSPLCompatibleName(name);

            sb.append("as ");
            sb.append(name);
        }

        String kind = jstring(op, "kind");
        sb.append(" = ");
        sb.append(kind);
    }

    static void inputClause(JsonObject op, StringBuilder sb) {

        sb.append("  ( ");

        AtomicBoolean firstPort = new AtomicBoolean(true);

        objectArray(op, "inputs", input -> {

            if (!firstPort.getAndSet(false))
                sb.append("; ");

            AtomicBoolean firstStream = new AtomicBoolean(true);
            stringArray(input, "connections", name -> {
                if (!firstStream.getAndSet(false))
                    sb.append(", ");
                sb.append(getSPLCompatibleName(name));
            });
            
            String alias = jstring(input, "alias");
            if (alias != null) {
                sb.append(" as ");
                sb.append(getSPLCompatibleName(alias));
            }
        });

        sb.append(")\n");
    }

    static void windowClause(JsonObject op, StringBuilder sb) {

        AtomicBoolean firstWindow = new AtomicBoolean(true);

        objectArray(op, "inputs", input -> {

            JsonObject window = jobject(input, "window");
            if (window == null)
                return;

            String type = jstring(window, "type");
            if (TYPE_NOT_WINDOWED.equals(type))
                return;

            if (firstWindow.getAndSet(false))
                sb.append("  window\n");

            sb.append("    ");
            sb.append(getSPLCompatibleName(inputPortRef(input)));
            sb.append(":");
            switch (type) {
            case TYPE_SLIDING:
                sb.append("sliding,");
                break;
            case TYPE_TUMBLING:
                sb.append("tumbling,");
                break;
            default:
                throw new IllegalStateException(Messages.getString("GENERATOR_INTERNAL_ERROR"));
            }

            appendWindowPolicy(jstring(window, "evictPolicy"), window.get("evictConfig"),
                    jstring(window, "evictTimeUnit"), sb);

            String triggerPolicy = jstring(window, "triggerPolicy");
            if (triggerPolicy != null) {
                sb.append(", ");
                appendWindowPolicy(triggerPolicy, window.get("triggerConfig"), jstring(window, "triggerTimeUnit"), sb);
            }

            if (jboolean(window, "partitioned"))
                sb.append(", partitioned");

            sb.append(";\n");
        });

    }

    static void appendWindowPolicy(String policyName, JsonElement config, String timeUnit, StringBuilder sb) {
        switch (policyName) {
        case POLICY_COUNT:
            sb.append("count(");
            sb.append(config.getAsInt());
            sb.append(")");
            break;
        case POLICY_DELTA:
            break;
        case POLICY_NONE:
            break;
        case POLICY_PUNCTUATION:
            break;
        case POLICY_TIME: {
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

    private void paramClause(JsonObject graphConfig, JsonObject op, StringBuilder sb) {

        // VMArgs only apply to Java SPL operators.
        boolean isJavaOp = OpProperties.LANGUAGE_JAVA.equals(jstring(op, OpProperties.LANGUAGE));

        JsonArray vmArgs = null;
        if (isJavaOp && graphConfig.has(ContextProperties.VMARGS))
            vmArgs = GsonUtilities.array(graphConfig, ContextProperties.VMARGS);

        // determine if we need to inject submission param names and values
        // info.
        boolean addSPInfo = false;
        ParamsInfo stvOpParamInfo = stvHelper.getSplInfo();
        if (stvOpParamInfo != null) {
            Map<String, JsonObject> functionalOps = stvHelper.getFunctionalOps();
            if (functionalOps.containsKey(op.get("name").getAsString()))
                addSPInfo = true;
        }

        JsonObject params = jobject(op, "parameters");
        if (vmArgs == null && GsonUtilities.jisEmpty(params) && !addSPInfo) {
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
                // stringArray(param, "value", v -> fullVmArgs.);
                // objectArray(graphConfig, ContextProperties.VMARGS, v ->
                // fullVmArgs.add(v));
                vmArgs = fullVmArgs;
                continue;
            }
            sb.append("      ");
            sb.append(name);
            sb.append(": ");
            splValueSupportingSubmission(param, sb);
            sb.append(";\n");
        }

        if (vmArgs != null) {
            JsonObject tmpVMArgParam = new JsonObject();
            tmpVMArgParam.add("value", vmArgs);
            sb.append("      ");
            sb.append("vmArg");
            sb.append(": ");

            splValueSupportingSubmission(tmpVMArgParam, sb);
            sb.append(";\n");
        }

        if (addSPInfo) {
            sb.append("      ");
            sb.append(FunctionalOpProperties.NAME_SUBMISSION_PARAM_NAMES);
            sb.append(": ");
            sb.append(stvOpParamInfo.names);
            sb.append(";\n");

            sb.append("      ");
            sb.append(FunctionalOpProperties.NAME_SUBMISSION_PARAM_VALUES);
            sb.append(": ");
            sb.append(stvOpParamInfo.values);
            sb.append(";\n");
        }
    }

    private void splValueSupportingSubmission(JsonObject value, StringBuilder sb) {

        JsonElement type = value.get("type");
        if (value.has("type") && TYPE_SUBMISSION_PARAMETER.equals(type.getAsString())) {
            value = stvHelper.getSPLExpression(value);
        }

        SPLGenerator.value(sb, value);
    }

    private void outputAssignmentClause(JsonObject graphConfig, JsonObject op, StringBuilder sb) {

        StringBuilder allAssignmentsSb = new StringBuilder();

        objectArray(op, "outputs", output -> {

            if (!output.has("assigns"))
                return;

            JsonObject assigns = object(output, "assigns");

            if (GsonUtilities.jisEmpty(assigns))
                return;

            StringBuilder assignsSb = new StringBuilder();
            String name = jstring(output, "name");
            name = getSPLCompatibleName(name);
            assignsSb.append(name);
            assignsSb.append(":\n");

            AtomicBoolean seenOne = new AtomicBoolean();
            for (Entry<String, JsonElement> a : assigns.entrySet()) {
                String attr = a.getKey();
                JsonObject value = a.getValue().getAsJsonObject();
                if (seenOne.getAndSet(true))
                    assignsSb.append(",\n");
                assignsSb.append("  ");
                assignsSb.append(attr);
                assignsSb.append("=");
                splValueSupportingSubmission(value, assignsSb);

            }
            assignsSb.append(";\n");

            allAssignmentsSb.append(assignsSb);
        });

        if (allAssignmentsSb.length() != 0) {
            sb.append(" output\n");
            sb.append(allAssignmentsSb);
        }
    }

    static void configClause(JsonObject graphConfig, JsonObject op, StringBuilder sb) {

        if (!op.has(OpProperties.CONFIG))
            return;

        JsonObject config = jobject(op, OpProperties.CONFIG);

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

        if (config.has(PLACEMENT)) {
            JsonObject placement = jobject(config, PLACEMENT);
            StringBuilder sbPlacement = new StringBuilder();

            String colocateKey = jstring(placement, OpProperties.PLACEMENT_COLOCATE_KEY);
            //String colocateKey = colocateTags != null && colocateTags.size() >= 1 ? colocateTags.get(0).getAsString() : null;
            //String colocateTag = jstring(placement, OpProperties.PLACEMENT_COLOCATE_KEY);
            if (colocateKey != null) {
                JsonObject mapping = object(graphConfig, CFG_COLOCATE_TAG_MAPPING);               
                String colocationId = jstring(mapping, colocateKey);               
                JsonObject colocateIds = object(graphConfig, CFG_COLOCATE_IDS);
                JsonObject idInfo = object(colocateIds, colocationId);
                
                boolean absoluteColocate = jboolean(idInfo, "main");
                if (!absoluteColocate) {
                    int parallel = idInfo.get("parallel").getAsInt();
                    if (parallel >= 2)
                        absoluteColocate = true;
                }
                
                sbPlacement.append("      partitionColocation(");
                
                stringLiteral(sbPlacement, colocationId);
                if (!absoluteColocate) {
                    // Use getChannel() to remain within a channel.                   
                    SPLGenerator.value(sbPlacement, JParamTypes.TYPE_SPL_EXPRESSION,
                            new JsonPrimitive("+'$'+((rstring)getChannel())"));
                }
                
                sbPlacement.append(")\n");
            }

            Set<String> uniqueResourceTags = new HashSet<>();
            GsonUtilities.stringArray(placement, OpProperties.PLACEMENT_RESOURCE_TAGS, tag -> {
                if (!tag.isEmpty())
                    uniqueResourceTags.add(tag);
            });
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
     * Gets or creates a host pool at the graphConfig level corresponding to the
     * unique set of tags.
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
