package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SPLTYPE;
import static com.ibm.streamsx.topology.internal.core.ObjectSchemas.JSON_SCHEMA;

import java.util.HashMap;
import java.util.Map;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.builder.JParamTypes;
import com.ibm.streamsx.topology.internal.gson.JSON4JBridge;

public class SPLStreamBridge {
        
    public static <T> TStream<T> subscribe(Topology topology, String topic, Class<T> tupleTypeClass) {
        if (JSON4JBridge.isJson4JClass(tupleTypeClass))
            return subscribeJson4j(topology, topic, tupleTypeClass);
        
        if (ObjectSchemas.usesDirectSchema(tupleTypeClass))
            return subscribeDirect(topology, topic, tupleTypeClass);
        
        return subscribeJava(topology, topic, tupleTypeClass);
    }
    
    private static <T> TStream<T> subscribeJson4j(Topology topology, String topic, Class<T> tupleTypeClass) {
        
        BOutputPort rawSubscribe = rawSubscribe(topology, topic, JSON_SCHEMA);
        
        BOperatorInvocation asJson;
        try {
            asJson = JavaFunctional.addFunctionalOperator(
                    topology,
                    "ToJSON",
                    JavaFunctionalOps.MAP_KIND, Class.forName("com.ibm.streamsx.topology.spl.SPLStreamImpl.JsonString2JSON"));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
        
        JavaFunctional.connectTo(topology, rawSubscribe, tupleTypeClass, asJson, null);
        
        return new StreamImpl<T>(topology, asJson.addOutput(ObjectSchemas.JAVA_OBJECT_SCHEMA), tupleTypeClass);
    }
    private static <T> TStream<T> subscribeDirect(Topology topology, String topic, Class<T> tupleTypeClass) {
        
        BOutputPort rawSubscribe = rawSubscribe(topology, topic, ObjectSchemas.getMappingSchema(tupleTypeClass));
        
        return new StreamImpl<T>(topology, rawSubscribe, tupleTypeClass);
    }
    
    private static BOutputPort rawSubscribe(Topology topology, String topic, String schema) {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", topic);
        params.put("streamType", JParamTypes.create(TYPE_SPLTYPE, schema));
        
        BOperatorInvocation subscribeOp = topology.builder().addSPLOperator(
                "Subscribe", "com.ibm.streamsx.topology.topic::Subscribe", params);
        
        return subscribeOp.addOutput(schema);
    }
    
    private static <T> TStream<T> subscribeJava(Topology topology, String topic, Class<T> tupleTypeClass) {
        final String schema = ObjectSchemas.getMappingSchema(tupleTypeClass);
        Map<String, Object> params = new HashMap<>();
        params.put("topic", topic);
        params.put("class", tupleTypeClass.getName());
        params.put("streamType", JParamTypes.create(TYPE_SPLTYPE, schema));
        
        BOperatorInvocation subscribeOp = topology.builder().addSPLOperator(
                "Subscribe", "com.ibm.streamsx.topology.topic::SubscribeJava", params);
        
        return new StreamImpl<T>(topology, subscribeOp.addOutput(schema), tupleTypeClass);
    }
}
