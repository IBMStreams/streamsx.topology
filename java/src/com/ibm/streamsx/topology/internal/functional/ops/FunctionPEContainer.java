/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;

import com.ibm.streams.operator.ProcessingElement;
import com.ibm.streamsx.topology.function.FunctionContainer;

class FunctionPEContainer implements FunctionContainer {
    
    private final ProcessingElement pe;
    
    FunctionPEContainer(ProcessingElement pe) {
        this.pe = pe;
    }

    @Override
    public String getJobId() {
        return pe.getJobId().toString();
    }

    @Override
    public String getId() {
        return pe.getPEId().toString();
    }

    @Override
    public String getDomainId() {
        return pe.getDomainId();
    }

    @Override
    public String getInstanceId() {
        return pe.getInstanceId();
    }

    @Override
    public int getRelaunchCount() {
        return pe.getRelaunchCount();
    }

    @Override
    public InetAddress getConfiguredHost() throws UnknownHostException {
        return pe.getConfiguredHost();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, String> getApplicationConfiguration(String name) {
        if (!pe.isStandalone()) {

            try {
                Method gac = ProcessingElement.class.getMethod("getApplicationConfiguration", String.class);
                return (Map<String,String>) gac.invoke(pe, name);
            } catch (NoSuchMethodException e) {
            } catch (InvocationTargetException|IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return Collections.emptyMap();
    }
    
    @Override
    public String getJobName() {
        return pe.getJobName();
    }
}
