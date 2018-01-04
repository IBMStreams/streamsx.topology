package com.ibm.streamsx.topology.spl;

import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_ATTRIBUTE;
import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SPLTYPE;

import java.util.HashMap;
import java.util.Map;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streamsx.topology.builder.JParamTypes;

/**
 * Utilities to "hide" the Streams Java Operator API from the
 * core "pure" classes which need to work with no local Streams install.
 *
 */
class OpAPIUtil {
    
    static Map<String,? extends Object> fixParameters(Map<String,? extends Object> params) {
        if (params == null)
            return null;
        
        Map<String,Object> fp = new HashMap<>(params);
        
        // Iterator over params as we may modify fp
        for (String name : params.keySet()) {
            Object value = fp.get(name);
            if (value == null) {
                fp.put(name, SPL.createNullValue());
            } else if (value instanceof StreamSchema) {
                fp.put(name, JParamTypes.create(TYPE_SPLTYPE, ((StreamSchema) value).getLanguageType()));
            } else if (value instanceof Attribute) {
                fp.put(name, JParamTypes.create(TYPE_ATTRIBUTE, ((Attribute) value).getName()));
            }
        }
        return fp;
    }
}
