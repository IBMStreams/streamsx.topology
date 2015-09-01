package com.ibm.streamsx.topology.internal.core;

import java.util.HashMap;
import java.util.Map;

import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.spl.UString;
import com.ibm.streamsx.topology.spl.Unsigned.UnsignedByte;
import com.ibm.streamsx.topology.spl.Unsigned.UnsignedInteger;
import com.ibm.streamsx.topology.spl.Unsigned.UnsignedLong;
import com.ibm.streamsx.topology.spl.Unsigned.UnsignedShort;
import com.ibm.streamsx.topology.tuple.JSONAble;

/**
 * A specification for a value of type {@code T}
 * whose actual value is not defined until topology execution time.
 */
public class SubmissionParameter<T> implements Supplier<T>, JSONAble {
    private static final long serialVersionUID = 1L;
    private static abstract class AltTypeHandler {
        final Class<?> wrappedType;
        final String typeModifier;
        AltTypeHandler(Class<?> wrappedType, String typeModifier) {
            this.wrappedType = wrappedType;
            this.typeModifier = typeModifier;
        }
        abstract Object unwrap(Object value);
    }
    private static final Map<Class<?>, AltTypeHandler> altTypeMap = new HashMap<>();
    static {
        altTypeMap.put(UString.class, new AltTypeHandler(String.class, "ustring") {
                Object unwrap(Object value) {
                    return ((UString)value).value();
                }});
        altTypeMap.put(UnsignedByte.class, new AltTypeHandler(Byte.class, "unsigned") {
                Object unwrap(Object value) {
                    return ((UnsignedByte)value).value();
                }});
        altTypeMap.put(UnsignedShort.class, new AltTypeHandler(Short.class, "unsigned") {
                Object unwrap(Object value) {
                    return ((UnsignedShort)value).value();
                }});
        altTypeMap.put(UnsignedInteger.class, new AltTypeHandler(Integer.class, "unsigned") {
                Object unwrap(Object value) {
                    return ((UnsignedInteger)value).value();
                }});
        altTypeMap.put(UnsignedLong.class, new AltTypeHandler(Long.class, "unsigned") {
                Object unwrap(Object value) {
                    return ((UnsignedLong)value).value();
                }});
    }
    private final String name;
    private final Class<T> valueClass;
    private final T defaultValue;

    /*
     * A submission time parameter specification without a default value.
     * @param name submission parameter name
     * @param valueClass class object for {@code T}
     * @throws IllegalArgumentException if {@code name} is null or empty
     */
    public SubmissionParameter(String name, Class<T> valueClass) {
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("name");
        this.name = name;
        this.valueClass = valueClass;
        this.defaultValue = null;
    }

    /**
     * A submission time parameter specification with a default value.
     * @param name submission parameter name
     * @param defaultValue default value if parameter isn't specified.
     * @throws IllegalArgumentException if {@code name} is null or empty
     * @throws IllegalArgumentException if {@code defaultValue} is null
     */
    @SuppressWarnings("unchecked")
    public SubmissionParameter(String name, T defaultValue) {
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("name");
        if (defaultValue == null)
            throw new IllegalArgumentException("defaultValue");
        this.name = name;
        this.valueClass = (Class<T>) defaultValue.getClass();
        this.defaultValue = defaultValue;
    }

    @Override
    public T get() {
        return null;
    }
   
    public String getName() {
        return name;
    }
    
    public T getDefaultValue() {
        return defaultValue;
    }

    @Override
    public JSONObject toJSON() {
        // meet the requirements of BOperatorInvocation.setParameter()
        // and OperatorGenerator.parameterValue()
        OrderedJSONObject jo = new OrderedJSONObject();
        OrderedJSONObject jv = new OrderedJSONObject();
        jo.put("type", "submissionParameter");
        jo.put("value", jv);
        jv.put("name", name);
        AltTypeHandler h = altTypeMap.get(valueClass);
        if (h == null) {
            jv.put("valueClassName", valueClass.getCanonicalName());
            jv.put("defaultValue", defaultValue);
        }
        else {
            jv.put("typeModifier", h.typeModifier);
            jv.put("valueClassName", h.wrappedType.getCanonicalName());
            if (defaultValue == null)
                jv.put("defaultValue", null);
            else
                jv.put("defaultValue", h.unwrap(defaultValue));
        }
        return jo;
    }

    @Override
    public String toString() {
        return toJSON().toString();
    }
}
