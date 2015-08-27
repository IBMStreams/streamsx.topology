/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.messaging.mqtt;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.ibm.streamsx.topology.builder.BOperatorInvocation;

class Util {
    private static final Map<String, ParamHandler> paramHandlers = new HashMap<>();
    static  {
        paramHandlers.put("serverURI", new ParamHandler("serverURI"));
        paramHandlers.put("clientID", new ParamHandler("clientID"));
        paramHandlers.put("keepAliveInterval", new ParamHandler("keepAliveInterval", Integer.class));
        paramHandlers.put("commandTimeoutMsec", new ParamHandler("commandTimeout", Long.class));
        paramHandlers.put("reconnectDelayMsec", new ParamHandler("period", Long.class));
        paramHandlers.put("receiveBufferSize", new ParamHandler("messageQueueSize", Integer.class));
        paramHandlers.put("retain", new ParamHandler("retain", Boolean.class));
        paramHandlers.put("userName", new ParamHandler("userName"));
        paramHandlers.put("password", new ParamHandler("password"));
        paramHandlers.put("keyStore", new ParamHandler("keyStore"));
        paramHandlers.put("keyStorePassword", new ParamHandler("keyStorePassword"));
        paramHandlers.put("trustStore", new ParamHandler("trustStore"));
        paramHandlers.put("trustStorePassword", new ParamHandler("trustStorePassword"));
        // defaultOQS: see consumer and producer for different definitions
    }

    static class ParamHandler {
        @SuppressWarnings("unused")
        private Class<?> paramType;
        private final String paramName;
        ParamHandler(String paramName) {
            this(paramName, String.class);
        }
        ParamHandler(String paramName, Class<?> paramType) {
            this.paramName = paramName;
            this.paramType = paramType;
        }
        /** SPL op parameter name */
        String getName() {
            return paramName;
        }
        /** SPL op parameter value */
        Object getValue(Object value) {
            // we now require the caller to pass in the correct type.
            return value;
        }
    }
    
    static Map<String,String> toMap(Properties props) {
        Map<String,String> map = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            map.put(key,props.getProperty(key));
        }
        return map;
    }
    
    static Map<String,Object> configToSplParams(Map<String,Object> config,
            Map<String,ParamHandler> overrideHandlers) {
        if (overrideHandlers == null)
            overrideHandlers = Collections.emptyMap();
        Map<String,Object> params = new HashMap<>();
        for (Map.Entry<String,Object> e : config.entrySet()) {
            ParamHandler ph = overrideHandlers.get(e.getKey());
            if (ph == null)
                ph = paramHandlers.get(e.getKey());
            if (ph != null)
                params.put(ph.getName(), ph.getValue(e.getValue()));
            else
                params.put(e.getKey(), e.getValue());
        }
        return params;
    }
    
    static String toCsv(List<?> list) {
        StringBuilder sb = new StringBuilder();
        for(Object o : list) {
            if (sb.length() > 0)
                sb.append(',');
            sb.append(o.toString());
        }
        return sb.toString();
    }
    
    /**
     * This is in lieu of a "kind" based JavaPrimitive.invoke*() methods.
     * @param splStream
     * @param kind
     * @param className
     */
    static void tagOpAsJavaPrimitive(BOperatorInvocation op, String kind, String className) {
        op.json().put("runtime", "spl.java");
        op.json().put("kind", kind);
        op.json().put("kind.javaclass", className);
    }

}
