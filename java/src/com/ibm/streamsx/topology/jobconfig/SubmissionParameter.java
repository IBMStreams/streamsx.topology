/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.jobconfig;

public class SubmissionParameter {
    
    private final String name;
    private String value;
    
    public SubmissionParameter(String name, boolean value) {
        this.name = name;
        this.value= Boolean.toString(value);
    }
    public SubmissionParameter(String name, String value) {
        this.name = name;
        this.value= value;
    }
    
    public SubmissionParameter(String name, Number value) {
        this.name = name;
        this.value = value.toString();
    }

    
    
    public String getName() {
        return name;
    }
    
    public String getValue() {
        return value;
    }
    
    public void setValue(String value) {
        this.value = value;
    }
    public void setValue(Number value) {
        this.value = value.toString();
    }
}
