package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE_JAVA;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_FUNCTIONAL;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BOperator;

public interface JavaFunctionalOps {
    
    static boolean isFunctional(BOperator op) {
        return LANGUAGE_JAVA.equals(jstring(op._json(), LANGUAGE))
                && MODEL_FUNCTIONAL.equals(jstring(op._json(), MODEL));
    }
    

    /**
     * Default namespace for functional operators.
     * A library that uses the SPI interface must
     * set their own namespace for Java functional operators
     * extended from this toolkit.
     */
    String NS = "com.ibm.streamsx.topology.functional.java";
    String NS_COLON = NS+"::";
    
    String PKG_O = "com.ibm.streamsx.topology.internal.functional.ops.";

    String AGGREGATE = PKG_O + "FunctionAggregate";
    String AGGREGATE_KIND = NS_COLON + "Aggregate";
    
    String CONVERT_SPL = PKG_O + "FunctionConvertToSPL";
    String CONVERT_SPL_KIND = NS_COLON + "ToSPL";
    
    String FILTER_KIND = NS_COLON + "Filter";
    
    String FLAT_MAP_KIND = NS_COLON + "FlatMap";
    
    String HASH_ADDER_KIND = NS_COLON + "HashAdder";
    
    String HASH_REMOVER_KIND = NS_COLON + "HashRemover"; // Technically not a functional op.

    String JOIN_KIND = NS_COLON + "Join";
    
    String MAP_KIND = NS_COLON + "Map";
    
    String PASS_KIND = NS_COLON + "PassThrough"; // Technically not a functional op.
    
    String PERIODIC_MULTI_SOURCE_KIND = NS_COLON + "FunctionPeriodicSource";
    
    String SPLIT_KIND = NS_COLON + "Split";
    
    
    String PKG = "com.ibm.streamsx.topology.internal.functional.operators.";    
   
    String FOR_EACH_KIND = NS_COLON + "ForEach";
    
    String SOURCE_KIND = NS_COLON + "Source";
    
    String PASS_CLASS = PKG + "PassThrough";
    
    static JsonObject kind2Class() {
        final JsonObject kinds = new JsonObject();
        
        kinds.addProperty(AGGREGATE_KIND, PKG_O + "FunctionAggregate");
        
        kinds.addProperty(CONVERT_SPL_KIND, PKG_O + "FunctionConvertToSPL");
        kinds.addProperty(FILTER_KIND, PKG_O + "FunctionFilter");
        
        kinds.addProperty(FLAT_MAP_KIND, PKG_O + "FunctionMultiTransform");
        

        kinds.addProperty(JOIN_KIND, PKG_O + "FunctionJoin");
        
        kinds.addProperty(MAP_KIND, PKG_O + "FunctionTransform");
        

        
        kinds.addProperty(PERIODIC_MULTI_SOURCE_KIND, PKG_O + "FunctionPeriodicSource");
        
        kinds.addProperty(SPLIT_KIND, PKG_O + "FunctionSplit");

        
        kinds.addProperty(FOR_EACH_KIND, PKG + "ForEach");
        
        kinds.addProperty(SOURCE_KIND, PKG + "Source");
        
        kinds.addProperty(PASS_KIND, PASS_CLASS);
        kinds.addProperty(HASH_ADDER_KIND, PKG + "HashAdder");      
        kinds.addProperty(HASH_REMOVER_KIND, PKG + "HashRemover");

        
        return kinds;
    }
}
