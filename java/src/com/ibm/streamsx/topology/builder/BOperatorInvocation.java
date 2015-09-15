/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streams.flow.declare.InputPortDeclaration;
import com.ibm.streams.flow.declare.OperatorInvocation;
import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.Operator;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.model.Namespace;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.tuple.JSONAble;

// Union(A,B)
//   OpC(A,B)
//   Union(A,B) --> 

// Parallel

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

    private final OperatorInvocation<? extends Operator> op;
    protected List<BInputPort> inputs;
    protected List<BOutputPort> outputs;
    private final JSONObject jparams = new OrderedJSONObject();

    public BOperatorInvocation(GraphBuilder bt,
            Class<? extends Operator> opClass,
            Map<String, ? extends Object> params) {
        super(bt);
        
        op = bt.graph().addOperator(opClass);
        json().put("name", op.getName());
        json().put("parameters", jparams);
        
        if (!Operator.class.equals(opClass)) {   
            json().put(JOperator.MODEL, JOperator.MODEL_SPL);
            json().put(JOperator.LANGUAGE, JOperator.LANGUAGE_JAVA);
            json().put("kind", getKind(opClass));
            json().put("kind.javaclass", opClass.getCanonicalName());
        }

        if (params != null) {
            for (String paramName : params.keySet()) {
                setParameter(paramName, params.get(paramName));
            }
        }
    }
    public BOperatorInvocation(GraphBuilder bt,
            String name,
            Class<? extends Operator> opClass,           
            Map<String, ? extends Object> params) {
        super(bt);
        op = bt.graph().addOperator(name, opClass);
        json().put("name", op.getName());
        json().put("parameters", jparams);
        
        if (!Operator.class.equals(opClass)) {   
            json().put(JOperator.MODEL, JOperator.MODEL_SPL);
            json().put(JOperator.LANGUAGE, JOperator.LANGUAGE_JAVA);
            json().put("kind", getKind(opClass));
            json().put("kind.javaclass", opClass.getCanonicalName());
        }

        if (params != null) {
            for (String paramName : params.keySet()) {
                setParameter(paramName, params.get(paramName));
            }
        }
    }

    public BOperatorInvocation(GraphBuilder bt,
            Map<String, ? extends Object> params) {
        this(bt, Operator.class, params);
    }
    public BOperatorInvocation(GraphBuilder bt,
            String name,
            Map<String, ? extends Object> params) {
        this(bt, name, Operator.class, params);
    }

    /*
    public BOperatorInvocation(GraphBuilder bt, String kind,
            Map<String, ? extends Object> params) {
        this(bt, Operator.class, params);
        json().put("kind", kind);
    }
    */

    public void setParameter(String name, Object value) {

        Object jsonValue = value;
        String jsonType = null;

        // Set the value in the OperatorInvocation.
        
        if (value instanceof JSONAble) {
            value = ((JSONAble)value).toJSON();
        }
        if (value instanceof JSONObject) {
            JSONObject jo = ((JSONObject) value);
            if (jo.get("type") == null || jo.get("value") == null)
                throw new IllegalArgumentException("Illegal JSONObject " + jo);
            String type = (String) jo.get("type");
            Object val = (JSONObject) jo.get("value");
            if ("__spl_value".equals(type)) {
                /*
                 * The Value object is
                 * <pre><code>
                 * object {
                 *   type : "__spl_value"
                 *   value : object {
                 *     value : any. non-null. type appropriate for metaType
                 *     metaType : com.ibm.streams.operator.Type.MetaType.name() string
                 *   }
                 * }
                 * </code></pre>
                 */
                // unwrap and fall through to handling for the wrapped value
                JSONObject splValue = (JSONObject) val;
                value = splValue.get("value");
                jsonValue = value;
                String metaType = (String) splValue.get("metaType");
                if ("USTRING".equals(metaType)
                        || "UINT8".equals(metaType)
                        || "UINT16".equals(metaType)
                        || "UINT32".equals(metaType)
                        || "UINT64".equals(metaType)) {
                    jsonType = metaType;
                }
                // fall through to handle jsonValue as usual 
            }
            else {
                // other kinds of JSONObject handled below
            }
        }
        else if (value instanceof Supplier<?>) {
            value = ((Supplier<?>) value).get();
            jsonValue = value;
        }
                
        if (value instanceof String) {
            op.setStringParameter(name, (String) value);
        } else if (value instanceof Byte) {
            op.setByteParameter(name, (Byte) value);
        } else if (value instanceof Short) {
            op.setShortParameter(name, (Short) value);
        } else if (value instanceof Integer) {
            op.setIntParameter(name, (Integer) value);
        } else if (value instanceof Long) {
            op.setLongParameter(name, (Long) value);
        } else if (value instanceof Float) {
            op.setFloatParameter(name, (Float) value);
        } else if (value instanceof Double) {
            op.setDoubleParameter(name, (Double) value);
        } else if (value instanceof Boolean) {
            op.setBooleanParameter(name, (Boolean) value);
        } else if (value instanceof BigDecimal) {
            op.setBigDecimalParameter(name, (BigDecimal) value);
            jsonValue = value.toString(); // Need to maintain exact value
        } else if (value instanceof Enum) {
            op.setCustomLiteralParameter(name, (Enum<?>) value);
            jsonValue = ((Enum<?>) value).name();
            jsonType = "enum";
        } else if (value instanceof StreamSchema) {
            jsonValue = ((StreamSchema) value).getLanguageType();
            jsonType = "spltype";
        } else if (value instanceof String[]) {
            String[] sa = (String[]) value;
            JSONArray a = new JSONArray(sa.length);
            for (String vs : sa)
                a.add(vs);
            jsonValue = a;
            op.setStringParameter(name, sa);
        } else if (value instanceof Attribute) {
            Attribute attr = (Attribute) value;
            jsonValue = attr.getName();
            jsonType = "attribute";
            op.setAttributeParameter(name, attr.getName());
        } else if (value instanceof JSONObject) {
            JSONObject jo = (JSONObject) value;
            jsonType = (String) jo.get("type");
            jsonValue = (JSONObject) jo.get("value");
        } else {
            throw new IllegalArgumentException("Type for parameter " + name + " is not supported:" +  value.getClass());
        }

        // Set the value as JSON
        JSONObject param = new JSONObject();
        param.put("value", jsonValue);

        if (jsonType != null)
            param.put("type", jsonType);

        jparams.put(name, param);
    }

    public BOutputPort addOutput(StreamSchema schema) {
        if (outputs == null)
            outputs = new ArrayList<>();

        final OutputPortDeclaration port = op.addOutput(schema);

        final BOutputPort stream = new BOutputPort(this, port);
        outputs.add(stream);
        return stream;
    }

    public BInputPort inputFrom(BOutput output, BInputPort input) {
        if (input != null) {
            assert input.operator() == this.op;
            assert inputs != null;

            output.connectTo(input);
            return input;
        }
        if (inputs == null) {
            inputs = new ArrayList<>();
        }

        InputPortDeclaration inputPort = op.addInput(this.op.getName() + "_IN" + inputs.size(),
                output.schema());
        input = new BInputPort(this, inputPort);
        inputs.add(input);
        output.connectTo(input);

        return input;
    }

    @Override
    public JSONObject complete() {
        final JSONObject json = super.complete();

        if (outputs != null) {
            JSONArray oa = new JSONArray(outputs.size());
            for (BOutputPort output : outputs) {
                oa.add(output.complete());
            }
            json.put("outputs", oa);
        }

        if (inputs != null) {
            JSONArray ia = new JSONArray(inputs.size());
            for (BInputPort input : inputs) {
                ia.add(input.complete());
            }
            json.put("inputs", ia);
        }

        return json;
    }

    // Needed by the DependencyResolver to determine whether the operator
    // has a 'jar' parameter by calling 
    //
    // op instance of FunctionFunctor
    public OperatorInvocation<? extends Operator> op() {
        return op;
    }

   
    private static String getKind(Class<?> opClass) {
        
        PrimitiveOperator primitive = opClass.getAnnotation(PrimitiveOperator.class);
        
        final String kindName = primitive.name().length() == 0 ?
                opClass.getSimpleName() : primitive.name();
        
        String namespace;
        if (primitive.namespace().length() != 0)
            namespace = primitive.namespace();
        else {
            Package pkg = opClass.getPackage();
            if (pkg != null) {
                Namespace ns = pkg.getAnnotation(Namespace.class);
                if (ns == null)
                    namespace = pkg.getName();
                else
                    namespace = ns.value();
            }
            else {
                namespace = "";
            }
        }
        
        return namespace + "::" + kindName;
    }
}
