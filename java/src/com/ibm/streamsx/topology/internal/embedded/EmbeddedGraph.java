package com.ibm.streamsx.topology.internal.embedded;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_FUNCTIONAL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_SPL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_VIRTUAL;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.NAME;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.NAMESPACE;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import com.google.gson.JsonObject;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.KIND;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.KIND_CLASS;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE_JAVA;

import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streams.flow.declare.OperatorGraphFactory;
import com.ibm.streams.flow.declare.OperatorInvocation;
import com.ibm.streams.operator.Operator;
import com.ibm.streamsx.topology.builder.BOperator;
import com.ibm.streamsx.topology.builder.GraphBuilder;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.generator.operator.OpProperties;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities;

/**
 * Takes the JSON graph defined by Topology
 * and creates an OperatorGraph for embedded use.
 * 
 * TODO - work in progress - currently just collects the operator decls.
 *
 */
public class EmbeddedGraph {
    
    private final GraphBuilder builder;
    private OperatorGraph graphDecl;
    
    public static void verifySupported(GraphBuilder builder) {
        new EmbeddedGraph(builder).verifySupported();
    }
   
    public EmbeddedGraph(GraphBuilder builder)  {
        this.builder = builder;
    }
    
    public void verifySupported() {        
        for (BOperator op : builder.getOps())
            verifyOp(op);
    }
    
    private void verifyOp(BOperator op) {
        JsonObject json = JSON4JUtilities.gson(op.complete());
        
        switch (jstring(json, MODEL)) {
        case MODEL_VIRTUAL:
            return;
        case MODEL_FUNCTIONAL:
        case MODEL_SPL:
            if (!LANGUAGE_JAVA.equals(jstring(json, LANGUAGE)))
                throw notSupported(op);
            break;
        default:
            throw notSupported(op);
        }
    }
    
    public OperatorGraph declareGraph() throws Exception {
        graphDecl = OperatorGraphFactory.newGraph();
        
        declareOps();
                
        return graphDecl;
    }

    private void declareOps() throws Exception {
        for (BOperator op : builder.getOps())
            declareOp(op);
    }
    
    @SuppressWarnings("unchecked")
    private void declareOp(BOperator op) throws Exception {
        JsonObject json = JSON4JUtilities.gson(op.complete());
        
        verifyOp(op);
        
        String opClassName = jstring(json, KIND_CLASS);
        Class<? extends Operator> opClass = (Class<? extends Operator>) Class.forName(opClassName);
        OperatorInvocation<? extends Operator> opDecl = graphDecl.addOperator(opClass);
    }
    
    private IllegalStateException notSupported(BOperator op) {
        
        String namespace = (String) builder.json().get(NAMESPACE);
        String name = (String) builder.json().get(NAME);
        
        return new IllegalStateException(
                "Topology '"+namespace+"."+name+"'"
                + " does not support "+StreamsContext.Type.EMBEDDED+" mode:"
                + " the topology contains non-Java operator:" + op.json().get(KIND));
    }
}
