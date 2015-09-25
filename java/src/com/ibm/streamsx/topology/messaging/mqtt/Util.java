/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.messaging.mqtt;

import java.util.HashMap;
import java.util.Map;

class Util {
    @SuppressWarnings("unused")
    private static final Util forCoverage = new Util();
    private static final Map<String, ParamHandler> paramHandlers = new HashMap<>();
    static  {
        paramHandlers.put("serverURI", new ParamHandler("serverURI"));
        paramHandlers.put("clientID", new ParamHandler("clientID"));
        paramHandlers.put("defaultQOS", new ParamHandler("qos", Integer.class));
        paramHandlers.put("keepAliveInterval", new ParamHandler("keepAliveInterval", Integer.class));
        paramHandlers.put("commandTimeoutMsec", new ParamHandler("commandTimeout", Long.class));
        paramHandlers.put("reconnectDelayMsec", new ParamHandler("period", Long.class));
        paramHandlers.put("receiveBufferSize", new ParamHandler("messageQueueSize", Integer.class));
        paramHandlers.put("retain", new ParamHandler("retain", Boolean.class));
        paramHandlers.put("userID", new ParamHandler("userID"));
        paramHandlers.put("password", new ParamHandler("password"));
        paramHandlers.put("keyStore", new ParamHandler("keyStore"));
        paramHandlers.put("keyStorePassword", new ParamHandler("keyStorePassword"));
        paramHandlers.put("trustStore", new ParamHandler("trustStore"));
        paramHandlers.put("trustStorePassword", new ParamHandler("trustStorePassword"));
    }
    
    private Util() { };

    private static class ParamHandler {
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
    
    static Map<String,Object> configToSplParams(Map<String,Object> config) {
        Map<String,Object> params = new HashMap<>();
        for (Map.Entry<String,Object> e : config.entrySet()) {
            ParamHandler ph = paramHandlers.get(e.getKey());
            if (ph != null)
                params.put(ph.getName(), ph.getValue(e.getValue()));
            else
                params.put(e.getKey(), e.getValue());
        }
        return params;
    }

}
