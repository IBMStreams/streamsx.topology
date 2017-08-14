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
    
    Type.MetaType metaType() {
        return metaType;
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
        switch (metaType) {
        case UINT8:
            if (value instanceof Byte) {
                jv.addProperty("value", Integer.toString(Byte.toUnsignedInt((Byte) value)));
                return jo;
            }
            break;
        case UINT16:
            if (value instanceof Short) {
                jv.addProperty("value", Integer.toString(Short.toUnsignedInt((Short) value)));
                return jo;
            }
            break;
        case UINT32:
            if (value instanceof Integer) {
                jv.addProperty("value", Long.toString(Integer.toUnsignedLong((Integer) value)));
                return jo;
            }
            break;
        case UINT64:
            if (value instanceof Long) {
                jv.addProperty("value", Long.toUnsignedString((Long) value));
                return jo;
            }
            break;
        default:
            break;
        }
        GsonUtilities.addToObject(jv, "value", value);
        return jo;
    }
}
