package com.ibm.streamsx.topology.internal.embedded;

import static com.ibm.streams.operator.Type.MetaType.BOOLEAN;
import static com.ibm.streams.operator.Type.MetaType.DECIMAL128;
import static com.ibm.streams.operator.Type.MetaType.FLOAT32;
import static com.ibm.streams.operator.Type.MetaType.FLOAT64;
import static com.ibm.streams.operator.Type.MetaType.INT16;
import static com.ibm.streams.operator.Type.MetaType.INT32;
import static com.ibm.streams.operator.Type.MetaType.INT64;
import static com.ibm.streams.operator.Type.MetaType.INT8;
import static com.ibm.streams.operator.Type.MetaType.RSTRING;
import static com.ibm.streams.operator.Type.MetaType.UINT16;
import static com.ibm.streams.operator.Type.MetaType.UINT32;
import static com.ibm.streams.operator.Type.MetaType.UINT64;
import static com.ibm.streams.operator.Type.MetaType.UINT8;
import static com.ibm.streams.operator.Type.MetaType.USTRING;
import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_ATTRIBUTE;
import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SUBMISSION_PARAMETER;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.KIND;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.KIND_CLASS;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE_JAVA;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_FUNCTIONAL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_SPL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_VIRTUAL;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.NAME;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.NAMESPACE;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.addAll;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jisEmpty;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectCreate;
import static com.ibm.streamsx.topology.spi.builder.Properties.Graph.CONFIG;
import static com.ibm.streamsx.topology.spi.builder.Properties.Graph.Config.JAVA_OPS;
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.streams.flow.declare.InputPortDeclaration;
import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streams.flow.declare.OperatorGraphFactory;
import com.ibm.streams.flow.declare.OperatorInvocation;
import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.Operator;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streamsx.topology.builder.BOperator;
import com.ibm.streamsx.topology.builder.GraphBuilder;
import com.ibm.streamsx.topology.builder.JParamTypes;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.generator.operator.OpProperties;
import com.ibm.streamsx.topology.internal.core.JavaFunctionalOps;
import com.ibm.streamsx.topology.internal.functional.SubmissionParameterManager;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.messages.Messages;

/**
 * Takes the JSON graph defined by Topology
 * and creates an OperatorGraph for embedded use.
 * 
 * TODO - work in progress - currently just collects the operator decls.
 *
 */
public class EmbeddedGraph {
    
    private final GraphBuilder builder;
    private final JsonObject kind2Class;
    private OperatorGraph graphDecl;
    
    // map for stream/port name to declared port.
    private final Map<String,OutputPortDeclaration> outputPorts = new HashMap<>();
    private final Map<String,InputPortDeclaration> inputPorts = new HashMap<>();
    
    private final JavaOperatorTester jot = new JavaOperatorTester();
    
    public static void verifySupported(GraphBuilder builder) {
        new EmbeddedGraph(builder).verifySupported();
    }
   
    public EmbeddedGraph(GraphBuilder builder)  {
        this.builder = builder;
        kind2Class = objectCreate(builder._json(), CONFIG, JAVA_OPS);
        addAll(kind2Class, JavaFunctionalOps.kind2Class());
    }
    
    public void verifySupported() {        
        for (BOperator op : builder.getOps())
            verifyOp(op._complete());
    }
    
    private boolean verifyOp(JsonObject op) {
        
        switch (jstring(op, MODEL)) {
        case MODEL_VIRTUAL:
            if (OpProperties.LANGUAGE_MARKER.equals(jstring(op, LANGUAGE)))
                return false;
            // fall through
        case MODEL_FUNCTIONAL:
        case MODEL_SPL:
            if (!LANGUAGE_JAVA.equals(jstring(op, LANGUAGE)))
                throw notSupported(op);
            return true;
        default:
            throw notSupported(op);
        }
    }
    
    public OperatorGraph declareGraph() throws Exception {
        assert graphDecl == null;
        
        graphDecl = OperatorGraphFactory.newGraph();
        
        declareOps();
        
        declareConnections();
                
        return graphDecl;
    }
    
    private JavaTestableGraph executionGraph;
    private Future<JavaTestableGraph> execution;
    public JavaTestableGraph getExecutionGraph() throws Exception {
        if (graphDecl == null)
            declareGraph();
        if (executionGraph == null)
            executionGraph = jot.executable(Objects.requireNonNull(graphDecl));
        return executionGraph;
    }
    public Future<JavaTestableGraph> execute() throws Exception {
        if (execution == null)
            execution = getExecutionGraph().execute();
        return execution;
    }

    private void declareOps() throws Exception {
        for (BOperator op : builder.getOps())
            declareOp(op);
    }
    
    /**
     * Creates the complete operator declaration
     * from the JSON representation.
     * @param op
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private void declareOp(BOperator op) throws Exception {
        JsonObject json = op._complete();
       
        if (!verifyOp(json))
            return;
        
        String opClassName = jstring(json, KIND_CLASS);
        if (opClassName == null) {
            opClassName = requireNonNull(
                    jstring(kind2Class, op.kind()), op.kind());
        }
        Class<? extends Operator> opClass = (Class<? extends Operator>) Class.forName(opClassName);
        OperatorInvocation<? extends Operator> opDecl = graphDecl.addOperator(opClass);
        
        if (json.has("parameters")) {
            JsonObject params = json.getAsJsonObject("parameters");
            for (Entry<String, JsonElement> param : params.entrySet())
                setOpParameter(opDecl, param.getKey(), param.getValue().getAsJsonObject());
        }
        
        declareOutputs(opDecl, json.getAsJsonArray("outputs"));
        declareInputs(opDecl, json.getAsJsonArray("inputs"));
    }
    
    private void declareOutputs(OperatorInvocation<? extends Operator> opDecl, JsonArray outputs) {
        if (GsonUtilities.jisEmpty(outputs))
            return;
        
        // Ensure we deal with them in port order.
        JsonObject[] ports = new JsonObject[outputs.size()];
        for (JsonElement e : outputs) {
            JsonObject output = e.getAsJsonObject();

            ports[output.get("index").getAsInt()] = output;
        }
        
        for (JsonObject output : ports) {
            String name = jstring(output, "name");            
            StreamSchema schema = Type.Factory.getTupleType(jstring(output, "type")).getTupleSchema();            
            OutputPortDeclaration port = opDecl.addOutput(name, schema);
          
            assert !outputPorts.containsKey(name);
            outputPorts.put(name, port);
        }  
    }
    private void declareInputs(OperatorInvocation<? extends Operator> opDecl, JsonArray inputs) {
        if (jisEmpty(inputs))
            return;
        
        // Ensure we deal with them in port order.
        JsonObject[] ports = new JsonObject[inputs.size()];
        for (JsonElement e : inputs) {
            JsonObject input = e.getAsJsonObject();

            ports[input.get("index").getAsInt()] = input;
        }
        
        for (JsonObject input : ports) {
            String name = jstring(input, "name");            
            StreamSchema schema = Type.Factory.getTupleType(jstring(input, "type")).getTupleSchema();            
            InputPortDeclaration port = opDecl.addInput(name, schema);
            
            assert !inputPorts.containsKey(name);
            inputPorts.put(name, port);
            
            if (input.has("window"))
                windowInput(input, port);
        }  
    }
    
    private void windowInput(JsonObject input, final InputPortDeclaration port) {
        JsonObject window = input.getAsJsonObject("window");
        String wt = jstring(window, "type");
        if (wt == null)
            return;
        StreamWindow.Type wtype = StreamWindow.Type.valueOf(wt);
        
        switch (wtype) {
        case NOT_WINDOWED:
            return;
        case SLIDING:
            port.sliding();
            break;
        case TUMBLING:
            port.tumbling();
            break;
        }
        
        StreamWindow.Policy evictPolicy = StreamWindow.Policy.valueOf(jstring(window, "evictPolicy"));
        
        // Eviction
        switch (evictPolicy) {
        case COUNT:
            int ecount = window.get("evictConfig").getAsInt();
            port.evictCount(ecount);
            break;
        case TIME:
            long etime = window.get("evictConfig").getAsLong();
            TimeUnit eunit = TimeUnit.valueOf(window.get("evictTimeUnit").getAsString());
            port.evictTime(etime, eunit);
            break;
        default:
            throw new UnsupportedOperationException(evictPolicy.name());
        }
        
        String stp = jstring(window, "triggerPolicy");      
        if (stp != null) {
            StreamWindow.Policy triggerPolicy = StreamWindow.Policy.valueOf(stp);
            switch (triggerPolicy) {
            case NONE:
                break;
            case COUNT:
                int tcount = window.get("triggerConfig").getAsInt();
                port.triggerCount(tcount);
                break;
            case TIME:
                long ttime = window.get("triggerConfig").getAsLong();
                TimeUnit tunit = TimeUnit.valueOf(window.get("triggerTimeUnit").getAsString());
                port.triggerTime(ttime, tunit);
                break;
            default:
                throw new UnsupportedOperationException(evictPolicy.name());
            }
        }
        
        if (window.has("partitioned") && window.get("partitioned").getAsBoolean())
            port.partitioned();
    }

    private void declareConnections() throws Exception {
        for (BOperator op : builder.getOps())
            declareOpConnections(op._complete());
    }

    private void declareOpConnections(JsonObject json) {
        JsonArray outputs = json.getAsJsonArray("outputs");
        if (jisEmpty(outputs))
            return;
        
        for (JsonElement e : outputs) {
            JsonObject output = e.getAsJsonObject();
            String name = jstring(output, "name");
            JsonArray conns = output.getAsJsonArray("connections");
            if (jisEmpty(conns))
                continue;
            
            OutputPortDeclaration port = requireNonNull(outputPorts.get(name));
            for (JsonElement c : conns) {
                String iname = c.getAsString();
                InputPortDeclaration iport = requireNonNull(inputPorts.get(iname));               
                port.connect(iport);
            }
        }     
    }

    /**
     * From a JSON parameter set the operator declaration parameter.
     */
    private void setOpParameter(OperatorInvocation<? extends Operator> opDecl, String name, JsonObject param)
    throws Exception
    {
        final JsonElement value = param.get("value");
        
        String type;
        
        if (param.has("type"))
            type = jstring(param, "type");
        else {
            type = "UNKNOWN";
            if (value.isJsonArray())
                type = RSTRING.name();
            else if (value.isJsonPrimitive()) {
                JsonPrimitive pv = value.getAsJsonPrimitive();
                if (pv.isBoolean())
                    type = BOOLEAN.name();
                else if (pv.isString())
                    type = RSTRING.name();
            }               
        }

        
        if (RSTRING.name().equals(type) || USTRING.name().equals(type)) {
            if (value.isJsonArray()) {
                JsonArray values = value.getAsJsonArray();
                String[] sv = new String[values.size()];
                for (int i = 0; i < sv.length; i++)
                    sv[i] = values.get(i).getAsString();
                opDecl.setStringParameter(name, sv);               
            } else
                opDecl.setStringParameter(name, value.getAsString());
        } else if (INT8.name().equals(type) || UINT8.name().equals(type))
            opDecl.setByteParameter(name, value.getAsByte());
        else if (INT16.name().equals(type) || UINT16.name().equals(type))
            opDecl.setShortParameter(name, value.getAsShort());
        else if (INT32.name().equals(type) || UINT32.name().equals(type))
            opDecl.setIntParameter(name, value.getAsInt());
        else if (INT64.name().equals(type) || UINT64.name().equals(type))
            opDecl.setLongParameter(name, value.getAsLong());
        else if (FLOAT32.name().equals(type))
            opDecl.setFloatParameter(name, value.getAsFloat());
        else if (FLOAT64.name().equals(type))
            opDecl.setDoubleParameter(name, value.getAsDouble());
        else if (BOOLEAN.name().equals(type))
            opDecl.setBooleanParameter(name, value.getAsBoolean());
        else if (DECIMAL128.name().equals(type))
            opDecl.setBooleanParameter(name, value.getAsBoolean());
        else if (TYPE_ATTRIBUTE.equals(type))
            opDecl.setAttributeParameter(name, value.getAsString());
        else if (JParamTypes.TYPE_ENUM.equals(type)) {
            final String enumClassName = param.get("enumclass").getAsString();
            final String enumName = value.getAsString();
            final Class<?> enumClass = Class.forName(enumClassName);
            if (enumClass.isEnum()) {
                for (Object eo : enumClass.getEnumConstants()) {
                    Enum<?> e = (Enum<?>) eo;
                    if (e.name().equals(enumName))
                        opDecl.setCustomLiteralParameter(name, e);
                }
            }          
            throw new IllegalArgumentException(Messages.getString("EMBEDDED_TYPE_OF_PARAM_NOT_SUPPOTRTED", name, type));
        } else
            throw new IllegalArgumentException(Messages.getString("EMBEDDED_TYPE_OF_PARAM_NOT_SUPPOTRTED", name, type));
    }
    
    private IllegalStateException notSupported(JsonObject op) {
        
        String namespace = jstring(builder._json(), NAMESPACE);
        String name = jstring(builder._json(), NAME);
        
        return new IllegalStateException(Messages.getString(
            "EMBEDDED_TOPOLOGY_NOT_SUPPORT_EMBEDDED",
            new String("'"+namespace+"."+name+"'"),
            StreamsContext.Type.EMBEDDED,
            jstring(op, KIND)));
    }

    public OutputPortDeclaration getOutputPort(String name) {
        OutputPortDeclaration portDecl = outputPorts.get(name); 
        return Objects.requireNonNull(portDecl);
    }

    /**
     * Initialize EMBEDDED submission parameter value information
     * from topology's graph and StreamsContext.submit() config.
     * @param builder the topology's builder
     * @param config StreamsContext.submit() configuration
     */
    public synchronized static void initializeEmbedded(GraphBuilder builder,
            Map<String, Object> config) {
    
        // N.B. in an embedded context, within a single JVM/classloader,
        // multiple topologies can be executed serially as well as concurrently.
        // TODO handle the concurrent case - e.g., with per-topology-submit
        // managers.
        
        // create map of all submission params used by the topology
        // and the parameter's string value (initially null for no default)
        Map<String,String> allsp = new HashMap<>();  // spName, spStrVal
        
        JsonObject gparams = GsonUtilities.object(builder._json(), "parameters");
        if (gparams != null) {
            for (Entry<String, JsonElement> sp : gparams.entrySet()) {
                JsonObject param = sp.getValue().getAsJsonObject();
                if (TYPE_SUBMISSION_PARAMETER.equals(jstring(param, "type"))) {
                    JsonObject spval = object(param, "value");
                    allsp.put(sp.getKey(), jstring(spval, "defaultValue"));
                }
            }
        }
        if (allsp.isEmpty())
            return;
        
        // update values from config
        @SuppressWarnings("unchecked")
        Map<String,Object> spValues =
            (Map<String, Object>) config.get(ContextProperties.SUBMISSION_PARAMS);
        if (spValues != null) {
            for (String spName : spValues.keySet()) {
                if (allsp.containsKey(spName)) {
                    Object val = spValues.get(spName);
                    if (val != null)
                        val = val.toString();
                    allsp.put(spName, (String)val);
                }
            }
        }
        
        // failure if any are still undefined
        for (String spName : allsp.keySet()) {
            if (allsp.get(spName) == null)
                throw new IllegalStateException(Messages.getString("EMBEDDED_PARAMETER_REQUIRED", spName));
        }
        
        // good to go. initialize params
        SubmissionParameterManager.setValues(allsp);
    }
}
