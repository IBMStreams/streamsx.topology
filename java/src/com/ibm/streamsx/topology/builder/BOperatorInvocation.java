/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.KIND;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectCreate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.generator.operator.OpProperties;
import com.ibm.streamsx.topology.internal.core.SubmissionParameterFactory;
import com.ibm.streamsx.topology.internal.functional.SPLTypes;
import com.ibm.streamsx.topology.internal.functional.SubmissionParameter;
import com.ibm.streamsx.topology.internal.messages.Messages;

/**
 * JSON representation.
 * 
 * Operator:
 * 
 * params: Object of Parameters, keyed by parameter name. outputs: Array of
 * output ports (see BStream) inputs: Array in input ports type: connections:
 * array of connection names as strings
 * 
 * Parameter value: Value of parameter.
 */

public class BOperatorInvocation extends BOperator {

    private List<BInputPort> inputs;
    private List<BOutputPort> outputs;
    private final JsonObject jparams = new JsonObject();

    public BOperatorInvocation(GraphBuilder bt,
            String kind,           
            Map<String, ? extends Object> params) {
        super(bt);
        _json().add("parameters", jparams);
        if (kind != null)
            _json().addProperty(KIND, kind);

        if (params != null) {
            for (String paramName : params.keySet()) {
                setParameter(paramName, params.get(paramName));
            }
        }
    }
    
    BOperatorInvocation(GraphBuilder bt,
            Map<String, ? extends Object> params) {
        this(bt, null, params);
    }
    
    public void setModel(String model, String language) {
        _json().addProperty(MODEL, model);
        _json().addProperty(LANGUAGE, language);
    }
    
    public String name() {
        return jstring(_json(), "name");
    }
    
    void rename(String newName) {
        
        String uniqueName = builder().userSuppliedName(newName);

        if (!uniqueName.equals(newName)) {
            JsonObject nameMap = new JsonObject();
            nameMap.addProperty(uniqueName, newName);
            layout().add("names", nameMap);
        }   
        
        // With a single output port operator its
        // stream (output port) and operator name can be the same.
        if (outputs != null && outputs.size() == 1) {
            BOutputPort singleOut = outputs.get(0);
            if (singleOut.name().equals(name())) {
                
                // Currently renaming only when it has no connections
                // as connections are made using simply the name (identifier) of
                // of the output object, not the object. Thus to rename the
                // port we would need to ensure existing connections
                // are updated to use the new name.
                if (array(singleOut._json(), "connections").size() == 0) {
                    singleOut._json().addProperty("name", uniqueName);
                } else {
                    // TODO: Rename when it has connections.
                }
            }
        }
        
        _json().addProperty("name", uniqueName);
    }

    public void setParameter(String name, Object value) {
        
        if (value == null)
            throw new IllegalStateException(Messages.getString("BUILDER_NULL_PARAM", name));
                
        if (value instanceof SubmissionParameter) {
            JsonObject svp = SubmissionParameterFactory.asJSON((SubmissionParameter<?>) value);
            /*
            JsonObject param = new JsonObject();
            param.add("type", svp.get("type"));
            param.add("value", svp.get("value"));         
            */ 
            jparams.add(name, svp);
            return;
        }
        
        Object jsonValue = value;
        String jsonType = null;

        if (value instanceof JsonObject) {
            JsonObject jo = ((JsonObject) value);
            if (!jo.has("type") || !jo.has("value"))
                throw new IllegalArgumentException(Messages.getString("BUILDER_ILLEGAL_JSON_OBJECT", jo));
            String type = jstring(jo, "type");
            if ("__spl_value".equals(type)) {
                /*
                 * The Value object is
                 * <pre><code>
                 * object {
                 *   type : "__spl_value"
                 *   value : object {
                 *     value : any. non-null. value appropriate for metaType
                 *     metaType : com.ibm.streams.operator.Type.MetaType.name() string
                 *   }
                 * }
                 * </code></pre>
                 */
                // unwrap and fall through to handling for the wrapped value
                JsonObject splValue = object(jo, "value");
                value = splValue.get("value");
                jsonValue = value;
                jsonType = jstring(splValue, "metaType");
                /*
                String metaType = jstring(splValue, "metaType");
                if ("USTRING".equals(metaType)
                        || "UINT8".equals(metaType)
                        || "UINT16".equals(metaType)
                        || "UINT32".equals(metaType)
                        || "UINT64".equals(metaType)) {
                    jsonType = metaType;
                }
                */
                // fall through to handle jsonValue as usual 
            }
            else {
                jparams.add(name, jo);
                return;
            }
        }
        else if (value instanceof Supplier<?>) {
            value = ((Supplier<?>) value).get();
            jsonValue = value;
        }
                
        if (value instanceof String) {
            if (jsonType == null)
                jsonType = SPLTypes.RSTRING;
        } else if (value instanceof Byte) {
            if (jsonType == null)
                jsonType = SPLTypes.INT8;
        } else if (value instanceof Short) {
            if (jsonType == null)
                jsonType = SPLTypes.INT16;
        } else if (value instanceof Integer) {
            if (jsonType == null)
                jsonType = SPLTypes.INT32;
        } else if (value instanceof Long) {
            if (jsonType == null)
                jsonType = SPLTypes.INT64;
        } else if (value instanceof Float) {
            jsonType = SPLTypes.FLOAT32;
        } else if (value instanceof Double) {
            jsonType = SPLTypes.FLOAT64;
        } else if (value instanceof Boolean) {
            jsonType = SPLTypes.BOOLEAN;
        } else if (value instanceof BigDecimal) {
            jsonValue = value.toString(); // Need to maintain exact value
            jsonType = SPLTypes.DECIMAL128;
        } else if (value instanceof Enum) {
            jsonValue = ((Enum<?>) value).name();
            jsonType = JParamTypes.TYPE_ENUM;
        } else if (value instanceof String[]) {
            String[] sa = (String[]) value;
            JsonArray a = new JsonArray();
            for (String vs : sa)
                a.add(new JsonPrimitive(vs));
            jsonValue = a;
        } else if (value instanceof JsonElement) {
            assert jsonType != null;
        } else {
            throw new IllegalArgumentException(Messages.getString("BUILDER_TYPE_OF_PARAMER_NOT_SUPPORTED", name, value.getClass()));
        }
        
        JsonObject param = JParamTypes.create(jsonType, jsonValue);
        
        jparams.add(name, param);
    }
    
    /**
     * Get the raw value for a parameter for this operator invocation.
     * @return Json representation of parameter or null if the parameter is not set.
     */
    public JsonObject getRawParameter(String name) {
        if (jparams.has(name))
            return jparams.getAsJsonObject(name);
        return null;             
    }

    public BOutputPort addOutput(String schema) {
        return addOutput(schema, Optional.empty());
    }
    
    public BOutputPort addOutput(String schema, Optional<String> name) {
    
        if (outputs == null)
            outputs = new ArrayList<>();
        
        String portName;
        if (name.isPresent())
            portName = name.get();
        else {
            portName = this.name() + "_OUT" + outputs.size();
            // Just in case the resultant name does clash with an operator name.
            portName = builder().userSuppliedName(portName);
        }
        
        final BOutputPort stream = new BOutputPort(this, outputs.size(),
                portName,
                schema);
        assert !outputs.stream().anyMatch(output -> stream.name().equals(output.name()));
        outputs.add(stream);
        return stream;
    }

    /**
     * Each input port has a unique generated name to ensure connections
     * are tracked correctly. However the port is not assigned
     * that name during SPL generation, instead taking the name
     * from its connected output stream (and the first if there
     * are multiple). The port may also have an alias which
     * will be used as the port alias (local to the operator).
     */
    public BInputPort inputFrom(BOutput output, BInputPort input) {
        if (input != null) {
            assert input.operator() == this;
            assert inputs != null;

            output.connectTo(input);
            return input;
        }
        if (inputs == null) {
            inputs = new ArrayList<>();
        }

        input = new BInputPort(this, inputs.size(), output._type());
        inputs.add(input);
        output.connectTo(input);

        return input;
    }
    
    public JsonObject layout() {
        return objectCreate(_json(), "layout");
    }
    
    public BOperatorInvocation layoutKind(String kind) {
        layout().addProperty(OpProperties.KIND, kind);
        return this;
    }


    @Override
    public JsonObject _complete() {
        final JsonObject json = super._complete();

        if (outputs != null) {
            JsonArray oa = new JsonArray();
            // outputs array in java is in port order.
            for (int i = 0; i < outputs.size(); i++)
                oa.add(JsonNull.INSTANCE); // will be overwritten with port info
            for (BOutputPort output : outputs)
                oa.set(output.index(), output._complete());
            json.add("outputs", oa);
        }

        if (inputs != null) {
            JsonArray ia = new JsonArray();
            for (int i = 0; i < inputs.size(); i++)
                ia.add(JsonNull.INSTANCE); // will be overwritten with port info
            for (BInputPort input : inputs)
                ia.set(input.index(), input._complete());
            json.add("inputs", ia);
        }

        return json;
    }
}
