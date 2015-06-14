package com.ibm.streamsx.topology.internal.core;

public interface InternalProperties {
    
    String PREFIX = "topology.internal.";
    
    String SPL_PREFIX = PREFIX + "spl.";
    
    String TK_DIRS_JSON = "toolkits";
    String TK_DIRS = SPL_PREFIX + TK_DIRS_JSON;

}
