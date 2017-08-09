/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import com.google.gson.JsonObject;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streamsx.topology.generator.spl.SPLGenerator;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * An implementation private wrapper for values of any SPL type. 
 * <p>
 * See {@link SPL#createValue(Object, MetaType)
 *
 * @param <T> the SPLValue's type
 */
class SPLValue<T> {
    private final T value;
    private final Type.MetaType metaType;
    
    SPLValue(T value, MetaType metaType) {
        this.value = value;
        this.metaType = metaType;
    }
    
    private static boolean isUnsignedInt(MetaType metaType) {
        return metaType == MetaType.UINT8
                || metaType == MetaType.UINT16
                || metaType == MetaType.UINT32
                || metaType == MetaType.UINT64;
    }

    T value() {
        return value;
    }
    
    Type.MetaType metaType() {
        return metaType;
    }
    
    @Override
    public String toString() {
        if (isUnsignedInt(metaType))
            return SPLGenerator.unsignedString(value);
        return value.toString();
    }
    
    // throws if jo not produced by toJSON()
    static SPLValue<?> fromJSON(JsonObject jo) {
        String type = jstring(jo, "type");
        if (!"__spl_value".equals(type))
            throw new IllegalArgumentException(jo.toString());
        JsonObject value = object(jo, "value");
        SPLValue<?> splValue = new SPLValue<String>(
                jstring(value, "value"),
                MetaType.valueOf(jstring(value, "metaType")));
        return splValue;
    }
    
    JsonObject asJSON() {
        // meet the requirements of BOperatorInvocation.setParameter()
        /*
         * The Value object is
         * <pre><code>
         * object {
         *   type : "__spl_value"
         *   value : object {
         *     value : any. non-null. type appropriate for metaType
         *     metaType : com.ibm.streams.operator.Type.MetaType.name() string
         *   }
         * }
         * </code></pre>
         */
        JsonObject jo = new JsonObject();
        JsonObject jv = new JsonObject();
        jo.addProperty("type", "__spl_value");
        jo.add("value", jv);
        jv.addProperty("metaType", metaType.name());
        GsonUtilities.addToObject(jv, "value", value());
        return jo;
    }
}
