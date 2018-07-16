/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.KIND;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE_JAVA;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_SPL;
import static com.ibm.streamsx.topology.internal.core.InternalProperties.TOOLKITS;
import static com.ibm.streamsx.topology.spl.OpAPIUtil.fixParameters;
import static com.ibm.streamsx.topology.spl.SPLStreamImpl.newSPLStream;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.JParamTypes;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.context.remote.TkInfo;
import com.ibm.streamsx.topology.internal.core.SourceInfo;
import com.ibm.streamsx.topology.internal.core.SubmissionParameterFactory;
import com.ibm.streamsx.topology.internal.core.TSinkImpl;
import com.ibm.streamsx.topology.internal.functional.SubmissionParameter;
import com.ibm.streamsx.topology.internal.toolkit.info.IdentityType;
import com.ibm.streamsx.topology.internal.toolkit.info.ToolkitInfoModelType;
import com.ibm.streamsx.topology.internal.messages.Messages;

/**
 * Integration between Java topologies and SPL operator invocations. If the SPL
 * operator to be invoked is an SPL Java primitive operator then the methods of
 * {@link JavaPrimitive} must be used.
 * <BR>
 * An operator's kind is its namespace followed by the
 * operator name separated with '{@code ::}'. For example
 * the {@code FileSource} operator from the SPL Standard toolkit must be specified
 * as {@code spl.adapter::FileSource}.
 * <p>
 * When necessary use {@code createValue(T, MetaType)},
 * or {@code createNullValue()},
 * to create parameter values for SPL types.
 * For example:
 * <pre>{@code
 * Map<String,Object> params = ...
 * params.put("aInt32", 13);
 * params.put("aUInt32", SPL.createValue(13, MetaType.UINT32));
 * params.put("aRString", "some string value");
 * params.put("aUString", SPL.createValue("some ustring value", MetaType.USTRING));
 * ... = SPLPrimitive.invokeOperator(..., params);
 * }</pre>
 * <p>
 * In addition to the usual Java types used for operator parameter values,
 * a {@code Supplier<T>} parameter value may be specified.
 * Submission time parameters are passed in this manner.
 * See {@link #createSubmissionParameter(Topology, String, Object, boolean)}.
 * For example:
 * <pre>{@code
 * Map<String,Object> params = ...
 * params.put("aInt32", topology.createSubmissionParameter("int32Param", 13);
 * params.put("aUInt32", SPL.createSubmissionParameter(topology, "uint32Param", SPL.createValue(13, MetaType.UINT32), true);
 * params.put("aRString", topology.createSubmissionParameter("rstrParam", "some string value");
 * params.put("aUString", SPL.createSubmissionParameter(topology, "ustrParam", SPL.createValue("some ustring value", MetaType.USTRING), true);
 * params.put("aUInt8", SPL.createSubmissionParameter(topology, "uint8Param", SPL.createValue((byte)13, MetaType.UINT8), true);
 * ... = SPLPrimitive.invokeOperator(..., params);
 * }</pre>
 * <P>
 * <B>Note:</B> Invoking an SPL composite operator with internal
 * {@code config placement} clauses can cause logical topology constraints
 * declared by {@link SPLStream#isolate() isolate}, {@link SPLStream#lowLatency() lowLatency}
 * or {@link SPLStream#colocate(com.ibm.streamsx.topology.context.Placeable...) colocate}
 * to be violated. This is due to the config placement clause within the composite definition
 * overriding the invocation clauses generated for the logical constraints.
 * </P>
 */
public class SPL {
    
    /**
     * Create an SPL value wrapper object for the 
     * specified SPL {@code MetaType}.
     * <p>
     * Use of this is required to construct an SPL operator parameter value
     * whose SPL type is not implied from simple Java type.  e.g.,
     * a {@code String} value is interpreted as an SPL {@code rstring},
     * and {@code Byte,Short,Integer,Long} are interpreted as SPL signed integers.
     * @param value the value to wrap
     * @param metaType the SPL meta type
     * @return the wrapper object
     * @throws IllegalArgumentException if value is null or its class is
     *     not appropriate for {@code metaType}
     */
    public static <T> Object createValue(T value, MetaType metaType) {
        return new SPLValue<T>(value, metaType).asJSON();
    }
    
    /**
     * Create an SPL value wrapper object for an SPL null value.
     * <p>
     * Use of this is required to construct an SPL operator parameter
     * null value for an optional type.
     * @return the wrapper object
     * @since 1.10
     */
    public static Object createNullValue() {
        JsonObject jo = new JsonObject();
        jo.addProperty("type", JParamTypes.TYPE_SPL_EXPRESSION);
        jo.addProperty("value", "null");
        return jo;
    }
    
    private static SPLValue<?> createSPLValue(Object paramValue) {
        if (paramValue instanceof JsonObject) {
            SPLValue<?> splValue = SPLValue.fromJSON((JsonObject) paramValue);
            return splValue;
        }            
        throw new IllegalArgumentException(Messages.getString("SPL_PARAMETER_INVALID"));
    }

    /**
     * Create a submission parameter with or without a default value.
     * <p>
     * Use of this is required to construct a submission parameter for
     * an SPL operator parameter whose SPL type requires the use of
     * {@code createValue(Object, MetaType)}.
     * <p>
     * See {@link Topology#createSubmissionParameter(String, Class)} for
     * general information about submission parameters.
     * 
     * @param top the topology
     * @param name the submission parameter name
     * @param paramValue a value from {@code createValue()}
     * @param withDefault true to create a submission parameter with
     *      a default value, false to create one without a default value
     *      When false, the paramValue's wrapped value's value in ignored. 
     * @return the {@code Supplier<T>} for the submission parameter
     * @throws IllegalArgumentException if {@code paramValue} is not a
     *      value from {@code createValue()}.
     */
    public static <T> Supplier<T> createSubmissionParameter(Topology top,
            String name, Object paramValue, boolean withDefault) {
        SPLValue<?> splValue = createSPLValue(paramValue);
        @SuppressWarnings("unchecked")
        SubmissionParameter<T> sp = (SubmissionParameter<T>) SubmissionParameterFactory.create(name, splValue.asJSON(), withDefault);
        top.builder().createSubmissionParameter(name, SubmissionParameterFactory.asJSON(sp));
        return sp;
    }
    
    /**
     * Create an SPLStream from the invocation of an SPL operator
     * that consumes a stream.
     * 
     * @param kind
     *            SPL kind of the operator to be invoked.
     * @param input
     *            Stream that will be connected to the only input port of the
     *            operator
     * @param outputSchema
     *            SPL schema of the operator's only output port.
     * @param params
     *            Parameters for the SPL operator, ignored if it is null.
     * @return SPLStream the represents the output of the operator.
     */
    public static SPLStream invokeOperator(String kind, SPLInput input,
            StreamSchema outputSchema, Map<String, ? extends Object> params) {
        
        return invokeOperator(opNameFromKind(kind), kind, input,
                outputSchema, params);
    }

    /**
     * Create an SPLStream from the invocation of an SPL operator
     * that consumes a stream and produces a stream.
     * 
     * @param name Name for the operator invocation.
     * @param kind
     *            SPL kind of the operator to be invoked.
     * @param input
     *            Stream that will be connected to the only input port of the
     *            operator
     * @param outputSchema
     *            SPL schema of the operator's only output port.
     * @param params
     *            Parameters for the SPL operator, ignored if it is null.
     * @return SPLStream the represents the output of the operator.
     */
    public static SPLStream invokeOperator(String name, String kind,
            SPLInput input,
            StreamSchema outputSchema, Map<String, ? extends Object> params) {

        BOperatorInvocation op = input.builder().addSPLOperator(name, kind, fixParameters(params));
        SourceInfo.setSourceInfo(op, SPL.class);
        SPL.connectInputToOperator(input, op);
        return newSPLStream(input, op, outputSchema, true);
    }
    
    /**
     * Invoke an SPL operator with an arbitrary number
     * of input and output ports.
     * <BR>
     * Each input stream or window in {@code inputs} results in
     * a input port for the operator with the input port index
     * matching the position of the input in {@code inputs}.
     * If {@code inputs} is {@code null} or empty then the operator will not
     * have any input ports.
     * <BR>
     * Each SPL schema in {@code outputSchemas} an output port
     * for the operator with the output port index
     * matching the position of the schema in {@code outputSchemas}.
     * If {@code outputSchemas} is {@code null} or empty then the operator will not
     * have any output ports.
     * 
     * @param te Reference to Topology the operator will be in.
     * @param name Name for the operator invocation.
     * @param kind
     *            SPL kind of the operator to be invoked.
     * @param inputs Input streams to be connected to the operator. May be {@code null} if no input  streams are required.
     * @param outputSchemas Schemas of the output streams. May be {@code null} if no output streams are required.
     * @param params
     *            Parameters for the SPL Java Primitive operator, ignored if {@code null}.
     * @return List of {@code SPLStream} instances that represent the outputs of the operator.
     */
    public static List<SPLStream> invokeOperator(
            TopologyElement te,
            String name,
            String kind,
            List<? extends SPLInput> inputs,
            List<StreamSchema> outputSchemas,
            Map<String, ? extends Object> params) {
        
        BOperatorInvocation op = te.builder().addSPLOperator(name, kind, fixParameters(params));
        
        SourceInfo.setSourceInfo(op, SPL.class);
        
        if (inputs != null && !inputs.isEmpty()) {
            for (SPLInput input : inputs)
                SPL.connectInputToOperator(input, op);
        }
        
        if (outputSchemas == null || outputSchemas.isEmpty())
            return Collections.emptyList();
        
        List<SPLStream> streams = new ArrayList<>(outputSchemas.size());
        for (StreamSchema outputSchema : outputSchemas)
            streams.add(newSPLStream(te, op, outputSchema, outputSchemas.size() == 1));
            
        return streams;
    }
    

    /**
     * Connect an input to an operator invocation, including making the input
     * windowed if it is an instance of SPLWindow.
     */
    static BInputPort connectInputToOperator(SPLInput input,
            BOperatorInvocation op) {
        BInputPort inputPort = input.getStream().connectTo(op, false, null);
        if (input instanceof SPLWindow) {
            ((SPLWindowImpl) input).windowInput(inputPort);
        }
        return inputPort;
    }

    /**
     * Invocation an SPL operator that consumes a Stream.
     * 
     * @param kind
     *            SPL kind of the operator to be invoked.
     * @param input
     *            Stream that will be connected to the only input port of the
     *            operator
     * @param params
     *            Parameters for the SPL operator, ignored if it is null.
     * @return the sink element
     */
    public static TSink invokeSink(String kind, SPLInput input,
            Map<String, ? extends Object> params) {

        return invokeSink(opNameFromKind(kind), kind, input, params);
    }

    /**
     * Invocation an SPL operator that consumes a Stream.
     * 
     * @param name
     *            Name of the operator
     * @param kind
     *            SPL kind of the operator to be invoked.
     * @param input
     *            Stream that will be connected to the only input port of the
     *            operator
     * @param params
     *            Parameters for the SPL operator, ignored if it is null.
     * @return the sink element
     */
    public static TSink invokeSink(String name, String kind, SPLInput input,
            Map<String, ? extends Object> params) {

        BOperatorInvocation op = input.builder().addSPLOperator(name, kind,
                fixParameters(params));
        SourceInfo.setSourceInfo(op, SPL.class);
        SPL.connectInputToOperator(input, op);
        return new TSinkImpl(input.topology(), op);
    }
    
    private static String opNameFromKind(String kind) {
        String opName;
        if (kind.contains("::"))
            opName = kind.substring(kind.lastIndexOf(':') + 1);
        else
            opName = kind;
        return opName;
    }

    /**
     * Invocation an SPL source operator to produce a Stream.
     * 
     * @param te
     *            Reference to Topology the operator will be in.
     * @param kind
     *            SPL kind of the operator to be invoked.
     * @param params
     *            Parameters for the SPL operator.
     * @param schema
     *            Schema of the output port.
     * @return SPLStream the represents the output of the operator.
     */
    public static SPLStream invokeSource(TopologyElement te, String kind,
            Map<String, Object> params, StreamSchema schema) {

        BOperatorInvocation splSource = te.builder().addSPLOperator(
                opNameFromKind(kind), kind, fixParameters(params));
        SourceInfo.setSourceInfo(splSource, SPL.class);
       
        return newSPLStream(te, splSource, schema, true);
    }

    /**
     * Add a dependency on an SPL toolkit.
     * @param te Element within the topology.
     * @param toolkitRoot Root directory of the toolkit.
     * @throws IOException {@code toolkitRoot} is not a valid path.
     */
    public static void addToolkit(TopologyElement te, File toolkitRoot) throws IOException {
            
        JsonObject tkinfo = newToolkitDepInfo(te);
        
        tkinfo.addProperty("root", toolkitRoot.getCanonicalPath());
        
        ToolkitInfoModelType infoModel;
        try {
            infoModel = TkInfo.getToolkitInfo(toolkitRoot);
        } catch (JAXBException e) {
            throw new IOException(e);
        }
        if (infoModel != null) {
            IdentityType tkIdentity = infoModel.getIdentity();

            tkinfo.addProperty("name", tkIdentity.getName());
            tkinfo.addProperty("version", tkIdentity.getVersion());
        }
    }
    
    private static JsonObject newToolkitDepInfo(TopologyElement te) {
        JsonArray tks = (JsonArray) te.topology().getConfig().get(TOOLKITS);
        
        if (tks == null) {
            te.topology().getConfig().put(TOOLKITS, tks = new JsonArray());
        }
        JsonObject tkinfo = new JsonObject();
        tks.add(tkinfo);
        return tkinfo;
    }
    
    /**
     * Add a logical dependency on an SPL toolkit.
     * @param te Element within the topology.
     * @param name Name of the toolkit.
     * @param version Version dependency string.
     */
    public static void addToolkitDependency(TopologyElement te, String name, String version) {
        JsonObject tkinfo = newToolkitDepInfo(te);
        tkinfo.addProperty("name", name);
        tkinfo.addProperty("version", version);
    }

    /**
     * Internal method.
     * <BR>
     * Not intended to be called by applications, may be removed at any time.
     * <br>
     * This is in lieu of a "kind" based JavaPrimitive.invoke*() methods.
     * @param op the operator invocation
     * @param kind SPL kind of the operator to be invoked.
     * @param className the Java primitive operator's class name.
     */
    public static void tagOpAsJavaPrimitive(BOperatorInvocation op, String kind, String className) {
        op._json().addProperty(MODEL, MODEL_SPL);
        op._json().addProperty(LANGUAGE, LANGUAGE_JAVA);
        op._json().addProperty(KIND, kind);
        op._json().addProperty("kind.javaclass", className);
    }
}
