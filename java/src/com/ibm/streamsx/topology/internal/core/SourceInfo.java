package com.ibm.streamsx.topology.internal.core;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;


public class SourceInfo {

    
    public static StackTraceElement getCaller(Class<?> calledClass) {

        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        
        boolean foundCalled = false;
        for (int i = 0; i < stack.length; i++) {
            StackTraceElement ste = stack[i];
            if (calledClass.getName().equals(ste.getClassName())) {
                foundCalled = true;
                continue;
            }
            
            if (foundCalled) {
                return ste;             
            }
        }
        
        return null;
    }
    
    
    public static void setSourceInfo(BOperatorInvocation bop, Class<?> calledClass) {
        
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        
        StackTraceElement calledMethod  = null;
        StackTraceElement caller  = null;
        
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
        
       
        JSONArray ja = (JSONArray) bop.json().get("sourcelocation");
        if (ja == null)
            bop.json().put("sourcelocation", ja = new JSONArray());
        JSONObject sourceInfo = new JSONObject();
        if (caller != null) {
            if (caller.getFileName() != null)
                sourceInfo.put("file", caller.getFileName());
            if (caller.getClassName() != null)
                sourceInfo.put("class", caller.getClassName());
            if (caller.getMethodName() != null)
                sourceInfo.put("method", caller.getMethodName());
            if (caller.getLineNumber() > 0)
                sourceInfo.put("line", caller.getLineNumber());
       }
        if (calledMethod != null)
            sourceInfo.put("topology.method", calledMethod.getMethodName());
        
        ja.add(sourceInfo);
    }
}
