/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streamsx.topology.builder.GraphBuilder;
import com.ibm.streamsx.topology.builder.JParamTypes;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.internal.embedded.EmbeddedGraph;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionFunctor;

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
 * call {@link FunctionFunctor#initializeSubmissionParameters(OperatorContext)}.
 * <p>
 * For EMBEDDED the functional operator's operator context does not have
 * a NAME_SUBMISSION_PARAMS parameter.  Instead the EMBEDDED
 * {@code StreamsContext.submit()} calls
 * {@link EmbeddedGraph#initializeEmbedded(GraphBuilder, Map)}.
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
    
    public static boolean initialized() {
        return params != UNINIT_MAP;
    }
        
    public static void setValues(Map<String,String> values) {
        params = values;
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
