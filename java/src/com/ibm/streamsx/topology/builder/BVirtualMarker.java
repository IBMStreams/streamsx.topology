package com.ibm.streamsx.topology.builder;

public enum BVirtualMarker {
    
    UNION("$Union$"),
    PARALLEL("$Parallel$"),
    END_PARALLEL("$EndParallel$"),
    LOW_LATENCY("$LowLatency$"),
    END_LOW_LATENCY("$EndLowLatency$"),
    ISOLATE("$Isolate$");
    
    private final String kind;
    
    private BVirtualMarker(String kind) {
        this.kind = kind;
    }
    
    public String kind() {
        return kind;
    }
    
    /**
     * Is the operator kind this virtual marker.
     */
    public boolean isThis(String kind) {
        return kind().equals(kind);
    }
    
    public static boolean isVirtualMarker(String kind) {
        if (kind == null)
            return false;
        
        if (!kind.startsWith("$")) {
            return false;
        }
        
        for (BVirtualMarker marker : BVirtualMarker.values()) {
            if (marker.kind.equals(kind))
                return true;
        }
        return false;
    }
}
