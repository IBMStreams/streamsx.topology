/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017  
 */
package com.ibm.streamsx.topology.spi;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public interface SourceInfo {

    String SOURCE_LOCATIONS = "sourcelocation";

    static void addSourceInfo(JsonObject config, Class<?> calledClass) {

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
            sourceInfo.addProperty("topology.method", calledMethod.getMethodName());

        if (!config.has(SOURCE_LOCATIONS)) {
            config.add(SOURCE_LOCATIONS, new JsonArray());
        }
        config.get(SOURCE_LOCATIONS).getAsJsonArray().add(sourceInfo);
    }

}
