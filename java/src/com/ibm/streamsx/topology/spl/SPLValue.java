/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streamsx.topology.generator.spl.SPLGenerator;

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
    
    public SPLValue(T value, MetaType metaType) {
        this.value = value;
        this.metaType = metaType;
    }
    
    private static boolean isUnsignedInt(MetaType metaType) {
        return metaType == MetaType.UINT8
                || metaType == MetaType.UINT16
                || metaType == MetaType.UINT32
                || metaType == MetaType.UINT64;
    }

    public T value() {
        return value;
    }
    
    public Type.MetaType metaType() {
        return metaType;
    }
    
    @Override
    public String toString() {
        if (isUnsignedInt(metaType))
            return SPLGenerator.unsignedString(value);
        return value.toString();
    }
    
    // throws if jo not produced by toJSON()
    public static SPLValue<?> fromJSON(JSONObject jo) {
        String type = (String) jo.get("type");
        if (!"__spl_value".equals(type))
            throw new IllegalArgumentException("jo");
        @SuppressWarnings({ "rawtypes", "unchecked" })
        SPLValue<?> splValue = new SPLValue(getWrappedValue(jo), getMetaType(jo));
        return splValue;
    }
    
    private static Object getWrappedValue(JSONObject jo) {
        JSONObject value = (JSONObject) jo.get("value");
        Object wrappedValue = value.get("value");
        return wrappedValue;
    }
    
    private static MetaType getMetaType(JSONObject jo) {
        JSONObject value = (JSONObject) jo.get("value");
        String metaType = (String) value.get("metaType");
        return MetaType.valueOf(metaType);
    }
    
    public JSONObject toJSON() {
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
        JSONObject jo = new OrderedJSONObject();
        JSONObject jv = new OrderedJSONObject();
        jo.put("type", "__spl_value");
        jo.put("value", jv);
        jv.put("metaType", metaType.name());
        jv.put("value", value);
        return jo;
    }
}