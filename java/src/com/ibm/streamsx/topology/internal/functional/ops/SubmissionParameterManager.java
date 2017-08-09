/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SUBMISSION_PARAMETER;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streamsx.topology.builder.GraphBuilder;
import com.ibm.streamsx.topology.builder.JParamTypes;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.generator.functional.FunctionalOpProperties;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * A manager for making a submission parameter Supplier usable
 * from functional logic.
 * <p>
 * The manager maintains a collection of values for every submission parameter
 * in the graph.
 * <p>
 * The submission parameter's {@code Supplier.get()} implementation learns
 * the actual submission parameter value by calling
 * {@link #getValue(String, String)}.
 * <p>
 * The strategy for the manager learning the submission parameter values
 * is as follows.
 * <p>
 * For DISTRIBUTED and STANDALONE (SPL code) submission parameter
 * values are conveyed via a NAME_SUBMISSION_PARAMS functional operator
 * parameter.  The parameter's value consists of all the submission parameters
 * created in the graph.  All functional operators have the same
 * value for this parameter in their OperatorContext.  Functional operators
 * call {@link #initialize(OperatorContext)}.
 * <p>
 * For EMBEDDED the functional operator's operator context does not have
 * a NAME_SUBMISSION_PARAMS parameter.  Instead the EMBEDDED
 * {@code StreamsContext.submit()} calls
 * {@link #initializeEmbedded(GraphBuilder, Map)}.
 * The collection of all submission parameters, with optional default values,
 * are learned from the graph and actual values are learned from the
 * submit configuration's {@link ContextProperties#SUBMISSION_PARAMS} value.
 */
public class SubmissionParameterManager {

    private static Map<String,java.util.function.Function<String, ?>> factories = new HashMap<>();
    static {
        factories.put(JParamTypes.RSTRING, s -> s);
        factories.put(JParamTypes.USTRING, factories.get(JParamTypes.RSTRING));
        factories.put(JParamTypes.INT8, s-> Byte.valueOf(s));
        factories.put(JParamTypes.INT16, s -> Short.valueOf(s));
        factories.put(JParamTypes.INT32, s -> Integer.valueOf(s));
        factories.put(JParamTypes.INT64, s -> Long.valueOf(s));
        factories.put(JParamTypes.UINT8, s -> Integer.valueOf(s).byteValue());
        factories.put(JParamTypes.UINT16, s-> Integer.valueOf(s).shortValue());
        factories.put(JParamTypes.UINT32, s-> Long.valueOf(s).intValue());
        factories.put(JParamTypes.UINT64, s -> new BigInteger(s).longValue());
        factories.put(JParamTypes.FLOAT32, s -> Float.valueOf(s));
        factories.put(JParamTypes.FLOAT64, s -> Double.valueOf(s));
    }
    private static final Map<String,String> UNINIT_MAP = Collections.emptyMap();
    /**  map of topology's <spOpParamName, strVal> */
    private static volatile Map<String,String> params = UNINIT_MAP;
    
    /** The name of the functional operator's actual SPL parameter
     * for the submission parameters names */
    public static final String NAME_SUBMISSION_PARAM_NAMES = FunctionalOpProperties.NAME_SUBMISSION_PARAM_NAMES;
    /** The name of the functional operator's actual SPL parameter
     * for the submission parameters values */
    public static final String NAME_SUBMISSION_PARAM_VALUES = FunctionalOpProperties.NAME_SUBMISSION_PARAM_VALUES;
    
    /**
     * Initialize submission parameter value information
     * from operator context information.
     * @param context the operator context
     */
    public synchronized static void initialize(OperatorContext context) {
        // The TYPE_SPL_SUBMISSION_PARAMS parameter value is the same for
        // all operator contexts.
        if (params == UNINIT_MAP) {
            List<String> names = context.getParameterValues(NAME_SUBMISSION_PARAM_NAMES);
            if (names != null && !names.isEmpty()) {
                List<String> values = context.getParameterValues(NAME_SUBMISSION_PARAM_VALUES);
                Map<String,String> map = new HashMap<>();
                for (int i = 0; i < names.size(); i++) {
                    String name = names.get(i);
                    String value = values.get(i);
                    map.put(name, value);
                }
                params = map;
                // System.out.println("SPM.initialize() " + params);
            }
        }
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
        for (String spName : spValues.keySet()) {
            if (allsp.containsKey(spName)) {
                Object val = spValues.get(spName);
                if (val != null)
                    val = val.toString();
                allsp.put(spName, (String)val);
            }
        }
        
        // failure if any are still undefined
        for (String spName : allsp.keySet()) {
            if (allsp.get(spName) == null)
                throw new IllegalStateException("Submission parameter \""+spName+"\" requires a value but none has been supplied");
        }
        
        // good to go. initialize params
        params = new HashMap<>();
        for (Map.Entry<String, String> e : allsp.entrySet()) {
            params.put(e.getKey(), e.getValue());
        }
        // System.out.println("SPM.initializeEmbedded() " + params);
    }

    /**
     * Get the submission parameter's value.
     * <p>
     * The value will be null while composing the topology.  It will be
     * the actual submission time value when the topology is running.
     * @param spName submission parameter name
     * @param metaType parameter's metaType
     * @return the parameter's value appropriately typed for metaType.
     *          may be null.
     */
    public static Object getValue(String spName, String metaType) {
        String value = params.get(spName);
        if (value == null) {
            // System.out.println("SPM.getValue "+spName+" "+metaType+ " params " + params);
            throw new IllegalArgumentException("Unexpected submission parameter name " + spName);
        }
        Function<String,?> factory = factories.get(metaType);
        if (factory == null)
            throw new IllegalArgumentException("Unhandled MetaType " + metaType);
        return factory.apply(value);
    }

}
