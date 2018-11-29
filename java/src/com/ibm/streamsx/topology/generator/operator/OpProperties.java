package com.ibm.streamsx.topology.generator.operator;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.arrayCreate;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectCreate;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public interface OpProperties {
    /**
     * Programming model used to create the operator.
     * Also used at the graph config level to provide
     * how the graph was created.
     */
    String MODEL = "model";
    
    /**
     * Language for the operator within its {@link #MODEL}.
     * Also used at the graph config level to provide
     * how the graph was created.
     */
    String LANGUAGE = "language";
    
    String MODEL_FUNCTIONAL = "functional";
    String MODEL_SPL = "spl";
    String MODEL_VIRTUAL = "virtual";
    
    String LANGUAGE_JAVA = "java";
    String LANGUAGE_CPP = "cpp";
    String LANGUAGE_PYTHON = "python";
    String LANGUAGE_SCALA = "scala";
    String LANGUAGE_SPL = "spl";
    String LANGUAGE_MARKER = "marker";
    
    String KIND = "kind";
    String KIND_CLASS = "kind.javaclass";
    
    /**
     * Boolean top-level parameter indicating
     * the operator is a starting point for
     * graph traversal. Added when a pending
     * stream is created to avoid not finding
     * a start (no input ports operator).
     */
    String START_OP = "startOp";

    /**
     * Boolean parameter indicating the operator is a HashAdder created
     * for a partitioned parallel region.
     */
    String HASH_ADDER = "hashAdder";
      
    /**
     * JSON attribute for operator configuration.
     */
    String CONFIG = "config"; 
    
    /**
     * Attribute for isolation region identifier.
     * A region that is on either side of an isolate
     * has a unique isolate region identifier.
     */
    String PLACEMENT_ISOLATE_REGION_ID = "isolateRegion";
    
    /**
     * Attribute for derived colocation key.
     * 
     * Selected from one of the tags in the placement during preprocessing.
     * Note at code generation the actual value to
     * use must be looked up from the map object
     * in the graph config.
     * 
     */
    String PLACEMENT_COLOCATE_KEY = "colocateIdKey";
    
    /**
     * List of colocation tags for each colocation the operator
     * is involved in. For example with operators A,B,C,D
     * and A colocated with B and D, A might have ['x','y']
     * B ['x'] and D ['y']. During preprocessing these tags
     * will be resolved to a single colocation identifier
     * used in the placement clause.
     * 
     * For any operator it will be colocated with another
     * if it shares at least one tag.
     * 
     * Colocations can come from explicit colocations
     * or low latency regions.
     */
    String PLACEMENT_COLOCATE_TAGS = "colocateTags";
    
    static void addColocationTag(JsonObject op, JsonPrimitive tag) {
        JsonObject placement = objectCreate(op, CONFIG, PLACEMENT);
        JsonArray colocateTags = arrayCreate(placement, PLACEMENT_COLOCATE_TAGS);
        colocateTags.add(tag);
    }

    /**
     * Attribute for an resource tags, a list of tags.
     */
    String PLACEMENT_RESOURCE_TAGS = "resourceTags";
    
    /**
     * Attribute for placement of the operator, a JSON object.
     * Can contain:
     * PLACEMENT_COLOCATE_TAGS
     * PLACEMENT_COLOCATE_KEY (after preprocessor)
     * PLACEMENT_RESOURCE_TAGS
     * 
     * Stored within {@link OpProperties#CONFIG}.
     */
    String PLACEMENT = "placement";
    
    /**
     * Attribute for start of a consistent region.
     */
    String CONSISTENT = "consistent";
    
	/** 
	 * Top-level boolean parameter indicating whether the operator is the start of a 
	 * parallel region.
	 */
	String PARALLEL = "parallel";
	
	/**
	 * The width of the parallel region, if the operator is the start of a parallel region.
	 */
	String WIDTH = "width";


}
