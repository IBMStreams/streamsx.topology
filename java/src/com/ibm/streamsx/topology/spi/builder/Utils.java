package com.ibm.streamsx.topology.spi.builder;

import java.util.HashMap;
import java.util.Map;

class Utils {
    
    static Map<String, Object> copyParameters(Map<String, Object> parameters) {
        Map<String, Object> ps = new HashMap<>();
        if (parameters != null)
            ps.putAll(parameters);
        
        return ps;     
    }

}
