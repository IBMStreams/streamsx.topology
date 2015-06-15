/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.generator.spl.SPLGenerator.splBasename;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streams.operator.window.StreamWindow.Type;
import com.ibm.streamsx.topology.context.ContextProperties;

class OperatorGenerator {

    static String generate(JSONObject graphConfig, JSONObject op) throws IOException {
        StringBuilder sb = new StringBuilder();
        noteAnnotations(op, sb);
        parallelAnnotation(op, sb);
        outputClause(op, sb);
        operatorNameAndKind(op, sb);
        inputClause(op, sb);

        sb.append("  {\n");
        windowClause(op, sb);
        paramClause(graphConfig, op, sb);
        configClause(graphConfig, op, sb);
        sb.append("  }\n");

        return sb.toString();
    }
    
    private static void noteAnnotations(JSONObject op, StringBuilder sb) throws IOException {
        JSONArray ja = (JSONArray) op.get("sourcelocation");
        if (ja == null || ja.isEmpty())
            return;
        
        JSONArtifact jsource = 
                ja.size() == 1 ? (JSONArtifact) ja.get(0) : ja;
        
        sb.append("@spl_note(id=\"__spl_sourcelocation\"");
        sb.append(", text=");
        String sourceInfo = jsource.serialize();
        SPLGenerator.stringLiteral(sb, sourceInfo);
        sb.append(")\n");
    }

    private static void parallelAnnotation(JSONObject op, StringBuilder sb) {
    	Boolean parallel = (Boolean)op.get("parallelOperator");
        if(parallel != null && parallel){
			sb.append("@parallel(width=" + Integer.toString((int)op.get("width")));
			Boolean partitioned = (Boolean)op.get("partitioned");
			if(partitioned != null && partitioned){
				String parallelInputPortName = (String) op.get("parallelInputPortName");
				parallelInputPortName = splBasename(parallelInputPortName);
				sb.append(", partitionBy=[{port=" + parallelInputPortName
						+ ", attributes=[__spl_hash]}]");
			}
			sb.append(")\n");
		}	
	}

	static void outputClause(JSONObject op, StringBuilder sb) {

        JSONArray outputs = (JSONArray) op.get("outputs");
        if (outputs == null || outputs.isEmpty()) {
            sb.append("() ");
            return;
        }

        sb.append("  ( ");
        for (int i = 0; i < outputs.size(); i++) {
            JSONObject output = (JSONObject) outputs.get(i);

            String type = (String) output.get("type");
            // removes the 'tuple' part of the type
            type = type.substring(5);

            String name = (String) output.get("name");
            name = splBasename(name);

            if (i != 0)
                sb.append("; ");

            sb.append("stream");
            sb.append(type);
            sb.append(" ");
            sb.append(name);
        }

        sb.append(") ");
    }

    static void operatorNameAndKind(JSONObject op, StringBuilder sb) {
        String name = (String) op.get("name");
        name = splBasename(name);

        sb.append("as ");
        sb.append(name);
        // sb.append("_op");
        /*
         * JSONArray outputs = (JSONArray) op.get("outputs"); if (outputs ==
         * null || outputs.isEmpty()) { sb.append("_sink"); }
         */

        String kind = (String) op.get("kind");
        sb.append(" = ");
        sb.append(kind);
    }

    static JSONArray getInputs(JSONObject op) {
        JSONArray inputs = (JSONArray) op.get("inputs");
        if (inputs == null || inputs.isEmpty())
            return null;
        return inputs;

    }

    static void inputClause(JSONObject op, StringBuilder sb) {

        JSONArray inputs = getInputs(op);
        if (inputs == null) {
            sb.append("()\n");
            return;
        }

        sb.append("  ( ");
        for (int i = 0; i < inputs.size(); i++) {
            JSONObject input = (JSONObject) inputs.get(i);

            JSONArray conns = (JSONArray) input.get("connections");

            if (i != 0)
                sb.append("; ");

            for (int j = 0; j < conns.size(); j++) {
                String name = (String) conns.get(j);
                name = splBasename(name);

                if (j != 0)
                    sb.append(", ");
                sb.append(name);
            }

            String name = (String) input.get("name");
            sb.append(" as ");
            sb.append(splBasename(name));
        }

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
            }

            appendWindowPolicy(window.get("evictPolicy"),
                    window.get("evictConfig"), sb);

            Object triggerPolicy = window.get("triggerPolicy");
            if (triggerPolicy != null) {
                sb.append(", ");
                appendWindowPolicy(triggerPolicy, window.get("triggerConfig"),
                        sb);
            }

            Boolean partitioned = (Boolean) window.get("partitioned");
            if (partitioned != null && partitioned) {
                sb.append(", partitioned");
            }
            sb.append(";\n");
        }

    }

    static void appendWindowPolicy(Object policyName, Object config,
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
            Long ms = (Long) config;
            double secs = ms.doubleValue() / 1000.0;
            sb.append("time(");
            sb.append(secs);
            sb.append(")");
            break;
        default:
            break;

        }
    }

    static void paramClause(JSONObject graphConfig, JSONObject op, StringBuilder sb) {
        
        JSONArray vmArgs = (JSONArray) graphConfig.get(ContextProperties.VMARGS);
        boolean hasVMArgs = vmArgs != null && !vmArgs.isEmpty();

        // VMArgs only apply to Java SPL operators.
        if (hasVMArgs) {
            if (!"spl.java".equals(op.get("runtime")))
                hasVMArgs = false;
        }

        JSONObject params = (JSONObject) op.get("parameters");
        if (!hasVMArgs && (params == null || params.isEmpty())) {
            return;
        }

        sb.append("    param\n");

        for (Object on : params.keySet()) {
            String name = (String) on;
            JSONObject param = (JSONObject) params.get(name);
            sb.append("      ");
            sb.append(name);
            sb.append(": ");
            parameterValue(param, sb);
            sb.append(";\n");
        }
        
        if (hasVMArgs) {
            JSONObject tmpVMArgParam = new JSONObject();
            tmpVMArgParam.put("value", vmArgs);
            sb.append("      ");
            sb.append("vmArg");
            sb.append(": ");

            parameterValue(tmpVMArgParam, sb);
            sb.append(";\n");
        }
    }
    
    // Set of "type"s where the "value" in the JSON is printed as-is.
    private static final Set<String> PARAM_TYPES_TOSTRING = new HashSet<>();
    static {
        PARAM_TYPES_TOSTRING.add("enum");
        PARAM_TYPES_TOSTRING.add("spltype");
        PARAM_TYPES_TOSTRING.add("attribute");    
    }

    static void parameterValue(JSONObject param, StringBuilder sb) {
        Object value = param.get("value");
        Object type = param.get("type");
        if (value instanceof String && !PARAM_TYPES_TOSTRING.contains(type)) {
            SPLGenerator.stringLiteral(sb, value.toString());
            return;
        } else if (value instanceof JSONArray) {
            JSONArray a = (JSONArray) value;
            for (int i = 0; i < a.size(); i++) {
                if (i != 0)
                    sb.append(", ");
                String sv = (String) a.get(i);
                SPLGenerator.stringLiteral(sb, sv);
            }
            return;
        } else if (value instanceof Number) {
            SPLGenerator.numberLiteral(sb, (Number) value);
            return;
        }

        sb.append(value);
    }
    
    static void configClause(JSONObject graphConfig, JSONObject op, StringBuilder sb) {
        JSONObject config = (JSONObject) op.get("config");
        if (config == null || config.isEmpty())
            return;
        
        Boolean streamViewability = (Boolean) config.get("streamViewability");
        if (streamViewability != null) {
            sb.append("  config\n");
            sb.append("    streamViewability: ");
            sb.append(streamViewability);
            sb.append(";\n");
        }
        
        
        
    }
}
