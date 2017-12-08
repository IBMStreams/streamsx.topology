/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SPLTYPE;
import static com.ibm.streamsx.topology.internal.functional.ObjectSchemas.JSON_SCHEMA;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.builder.JParamTypes;
import com.ibm.streamsx.topology.internal.functional.ObjectSchemas;
import com.ibm.streamsx.topology.internal.gson.JSON4JBridge;

/**
 * Provides a bridge between the TStream and SPL/SPLStream world using
 * SPL classes indirectly to avoid a dependency on the operator api.
 *
 */
public class SPLStreamBridge {
        
    public static <T> TStream<T> subscribe(Topology topology, Object topic, Class<T> tupleTypeClass) {
        if (JSON4JBridge.isJson4JClass(tupleTypeClass))
            return subscribeJson4j(topology, topic, tupleTypeClass);
        
        if (ObjectSchemas.usesDirectSchema(tupleTypeClass))
            return subscribeDirect(topology, topic, tupleTypeClass);
        
        return subscribeJava(topology, topic, tupleTypeClass);
    }
    
    private static <T> TStream<T> subscribeJson4j(Topology topology, Object topic, Class<T> tupleTypeClass) {
        
        BOutputPort rawSubscribe = rawSubscribe(topology, topic, JSON_SCHEMA);
        
        BOperatorInvocation asJson;
        try {
            asJson = JavaFunctional.addFunctionalOperator(
                    topology,
                    "ToJSON",
                    JavaFunctionalOps.MAP_KIND,
                    (Serializable) (Class.forName("com.ibm.streamsx.topology.internal.json4j.JSONTopoRuntime$JsonString2JSON").newInstance()));
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new IllegalStateException(e);
        }
        
        JavaFunctional.connectTo(topology, rawSubscribe, tupleTypeClass, asJson, null);
        
        return new StreamImpl<T>(topology, asJson.addOutput(ObjectSchemas.JAVA_OBJECT_SCHEMA), tupleTypeClass);
    }
    private static <T> TStream<T> subscribeDirect(Topology topology, Object topic, Class<T> tupleTypeClass) {
        
        BOutputPort rawSubscribe = rawSubscribe(topology, topic, ObjectSchemas.getMappingSchema(tupleTypeClass));
        
        return new StreamImpl<T>(topology, rawSubscribe, tupleTypeClass);
    }
    
    private static BOutputPort rawSubscribe(Topology topology, Object topic, String schema) {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", topic);
        params.put("streamType", JParamTypes.create(TYPE_SPLTYPE, schema));
        
        BOperatorInvocation subscribeOp = topology.builder().addSPLOperator(
                "Subscribe", "com.ibm.streamsx.topology.topic::Subscribe", params);
        
        return subscribeOp.addOutput(schema);
    }
    
    private static <T> TStream<T> subscribeJava(Topology topology, Object topic, Class<T> tupleTypeClass) {
        final String schema = ObjectSchemas.getMappingSchema(tupleTypeClass);
        Map<String, Object> params = new HashMap<>();
        params.put("topic", topic);
        params.put("class", tupleTypeClass.getName());
        params.put("streamType", JParamTypes.create(TYPE_SPLTYPE, schema));
        
        BOperatorInvocation subscribeOp = topology.builder().addSPLOperator(
                "Subscribe", "com.ibm.streamsx.topology.topic::SubscribeJava", params);
        
        return new StreamImpl<T>(topology, subscribeOp.addOutput(schema), tupleTypeClass);
    }
    
    static void publishJSON(TStream<?> stream, Object topic) {
        try {
            Class<?>  jsonStreams = Class.forName("com.ibm.streamsx.topology.json.JSONStreams");
            Class<?>  splStream = Class.forName("com.ibm.streamsx.topology.spl.SPLStream");
            Object asSPLStream = jsonStreams.getMethod("toSPL", TStream.class).invoke(null, stream);
            splStream.getMethod("publish", String.class, Boolean.TYPE).invoke(asSPLStream, topic, false);
        } catch (ClassNotFoundException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new IllegalStateException(e);
        }
        
    }
}
