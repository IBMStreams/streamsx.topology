package com.ibm.streamsx.topology.generator.operator;

public interface OpProperties {
    /**
     * Programming model used to create the operator.
     */
    String MODEL = "model";
    
    /**
     * Language for the operator within its {@link #MODEL}.
     */
    String LANGUAGE = "language";
    
    String MODEL_FUNCTIONAL = "functional";
    String MODEL_SPL = "spl";
    
    String LANGUAGE_JAVA = "java";
    String LANGUAGE_CPP = "cpp";
    String LANGUAGE_PYTHON = "python";
    String LANGUAGE_SPL = "spl";
      
    /**
     * JSON attribute for operator configuration.
     */
    String CONFIG = "config"; 
    
    /**
     * Attribute for isolation region identifier.
     */
    String PLACEMENT_ISOLATE_REGION_ID = "isolateRegion";

    /**
     * Attribute for an explicit colocation identifier.
     */
    String PLACEMENT_EXPLICIT_COLOCATE_ID = "explicitColocate";

    /**
     * Attribute for low latency region identifier.
     */
    String PLACEMENT_LOW_LATENCY_REGION_ID = "lowLatencyRegion";

    /**
     * Attribute for an resource tags, a list of tags.
     */
    String PLACEMENT_RESOURCE_TAGS = "resourceTags";
    
    /**
     * Attribute for placement of the operator, a JSON object.
     * Stored within {@link OpProperties#CONFIG}.
     */
    String PLACEMENT = "placement";


}
