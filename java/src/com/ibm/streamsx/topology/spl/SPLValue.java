/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import java.util.HashMap;
import java.util.Map;

import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streamsx.topology.generator.spl.SPLGenerator;

/**
 * An implementation private wrapper for values of any SPL type. 
 * <p>
 * See {@link SPL#createParamValue(Object, MetaType)
 *
 * @param <T> the SPLValue's type
 */
class SPLValue<T> {
    private static Map<MetaType,Info> mtypeMap = new HashMap<>();
    static {
        mtypeMap.put(MetaType.RSTRING, new Info(String.class, ""));
        mtypeMap.put(MetaType.USTRING, new Info(String.class, "", "utf16"));
        mtypeMap.put(MetaType.BOOLEAN, new Info(Boolean.class, false));
        mtypeMap.put(MetaType.INT8,    new Info(Byte.class, (byte)0));
        mtypeMap.put(MetaType.UINT8,   new Info(Byte.class, (byte)0, "unsigned"));
        mtypeMap.put(MetaType.INT16,   new Info(Short.class, (short)0));
        mtypeMap.put(MetaType.UINT16,  new Info(Short.class, (short)0, "unsigned"));
        mtypeMap.put(MetaType.INT32,   new Info(Integer.class, 0));
        mtypeMap.put(MetaType.UINT32,  new Info(Integer.class, 0, "unsigned"));
        mtypeMap.put(MetaType.INT64,   new Info(Long.class, (long)0));
        mtypeMap.put(MetaType.UINT64,  new Info(Long.class, (long)0, "unsigned"));
        mtypeMap.put(MetaType.FLOAT32, new Info(Float.class, (float)0.0));
        mtypeMap.put(MetaType.FLOAT64, new Info(Double.class, (double)0.0));
        // add more as needed
    }
    
    private static class Info {
        final Class<?> clazz;
        final String typeModifier;  // utf16, unsigned, null (the value's T speaks for itself)
        final Object defaultValue;  // default value for a clazz instance
        Info(Class<?> clazz, Object defaultValue, String typeModifier) {
            this.clazz = clazz;
            this.typeModifier = typeModifier;
            this.defaultValue = defaultValue;
        }
        Info(Class<?> clazz, Object defaultValue) {
            this(clazz, defaultValue, null);
        }
    }

    private final T value;
    private final Type.MetaType metaType;
    private final Info info;
    
    public SPLValue(T value, MetaType metaType) {
        this.value = value;
        this.metaType = metaType;
        this.info = mtypeMap.get(metaType);
        if (info == null)
            throw new IllegalArgumentException("Unhandled MetaType " + metaType);
        if (value.getClass() != info.clazz)
            throw new IllegalArgumentException("MetaType=" + metaType + " valueClass=" + value.getClass());
    }

    // value() will return a default value for the type
    @SuppressWarnings("unchecked")
    public SPLValue(MetaType metaType) {
        this.metaType = metaType;
        this.info = mtypeMap.get(metaType);
        if (info == null)
            throw new IllegalArgumentException("Unhandled MetaType " + metaType);
        this.value = (T) info.defaultValue;
    }
    
    public T value() {
        return value;
    }
    
    public Type.MetaType metaType() {
        return metaType;
    }
    
    @Override
    public String toString() {
        if (info.typeModifier.equals("unsigned"))
            return SPLGenerator.unsignedString(value);
        return value.toString();
    }
    
    // throws if jo not produced by toJSON()
    public static SPLValue<?> fromJSON(JSONObject jo) {
        String type = (String) jo.get("type");
        if (!"__spl_wrappedValue".equals(type))
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
        Object wrappedValue = value.get("value");
        String typeModifier = (String) value.get("typeModifier");

        Class<?> clazz = wrappedValue.getClass();
        for (Map.Entry<MetaType, Info> e : mtypeMap.entrySet()) {
            Info info = e.getValue(); 
            if (info.clazz == clazz 
                && (info.typeModifier == null
                    ? info.typeModifier == typeModifier
                    : info.typeModifier.equals(typeModifier)))
                return e.getKey();
        }
        throw new IllegalArgumentException("Unable to locate metaType for " + jo);
    }
    
    public JSONObject toJSON() {
        // meet the requirements of BOperatorInvocation.setParameter()
        /*
         * The WrappedValue parameter value object is
         * <pre><code>
         * object {
         *   type : "__spl_wrappedValue"
         *   value : object {
         *     value : any. non-null.
         *     typeModifier : optional null, "utf16", "unsigned"
         *   }
         * }
         * </code></pre>
         */
        JSONObject jo = new OrderedJSONObject();
        jo.put("type", "__spl_wrappedValue");
        JSONObject jval = new OrderedJSONObject();
        jo.put("value", jval);
        if (info.typeModifier != null)
            jval.put("typeModifier", info.typeModifier);
        jval.put("value", value);
        return jo;
    }
}