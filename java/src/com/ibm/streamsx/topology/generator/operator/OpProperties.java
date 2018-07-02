package com.ibm.streamsx.topology.generator.operator;

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
     * Attribute for an explicit colocation identifier.
     * 
     * An explicit colocate identifier is an instruction
     * from the application that it wants two (or more) operators
     * to be colocated.
     */
    String PLACEMENT_EXPLICIT_COLOCATE_ID = "explicitColocate";
    
    /**
     * Attribute for derived colocation key.
     * 
     * Selected from one of the tags in the placement.
     * Note at code generation the actual value to
     * use must be looked up from the map object
     * in the graph config.
     * 
     */
    String PLACEMENT_COLOCATE_KEY = "colocateIdKey";

    /**
     * Attribute for low latency region identifier.
     * 
     * A low latency region has a unique isolate region identifier.
     */
    String PLACEMENT_LOW_LATENCY_REGION_ID = "lowLatencyRegion";

    /**
     * Attribute for an resource tags, a list of tags.
     */
    String PLACEMENT_RESOURCE_TAGS = "resourceTags";
    
    /**
     * Attribute for placement of the operator, a JSON object.
     * Can contain:
     * PLACEMENT_ISOLATE_REGION_ID
     * PLACEMENT_EXPLICIT_COLOCATE_ID
     * PLACEMENT_LOW_LATENCY_REGION_ID
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
