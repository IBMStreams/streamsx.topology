/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017  
 */
package com.ibm.streamsx.topology.spi.builder;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Add the ability to add source location information to an operator invocation.
 *
 */
public interface SourceInfo {

    String SOURCE_LOCATIONS = "sourcelocation";

    /**
     * Add source level information to invokeInfo as the property {@value SOURCE_LOCATIONS}.
     * 
     * The current stack is "walked" upwards to find the
     * last instance of calledClass. The frame including
     * called class is taken as the 'api.method' while
     * the caller is taken as user code.
     * 
     * @param invokeInfo Object holding operator invoke information.
     * @param calledClass API implementation class being called.
     */
    static void addSourceInfo(JsonObject invokeInfo, Class<?> calledClass) {

        StackTraceElement[] stack = Thread.currentThread().getStackTrace();

        StackTraceElement calledMethod = null;
        StackTraceElement caller = null;

        boolean foundCalled = false;
        for (int i = 0; i < stack.length; i++) {
            StackTraceElement ste = stack[i];
            if (calledClass.getName().equals(ste.getClassName())) {
                foundCalled = true;
                calledMethod = ste;
                continue;
            }

            if (foundCalled) {
                caller = ste;
                break;
            }
        }

        JsonObject sourceInfo = new JsonObject();
        if (caller != null) {
            if (caller.getFileName() != null)
                sourceInfo.addProperty("file", caller.getFileName());
            if (caller.getClassName() != null)
                sourceInfo.addProperty("class", caller.getClassName());
            if (caller.getMethodName() != null)
                sourceInfo.addProperty("method", caller.getMethodName());
            if (caller.getLineNumber() > 0)
                sourceInfo.addProperty("line", caller.getLineNumber());
        }
        if (calledMethod != null)
            sourceInfo.addProperty("api.method", calledMethod.getMethodName());

        if (!invokeInfo.has(SOURCE_LOCATIONS)) {
            invokeInfo.add(SOURCE_LOCATIONS, new JsonArray());
        }
        invokeInfo.get(SOURCE_LOCATIONS).getAsJsonArray().add(sourceInfo);
    }

}
