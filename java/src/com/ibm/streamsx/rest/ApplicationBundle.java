/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
*/
package com.ibm.streamsx.rest;

import java.io.IOException;

import com.google.gson.JsonObject;

/**
 * 
 * An object describing an IBM Streams Domain
 * 
 * @since 1.11
 */
public abstract class ApplicationBundle extends Element {
	
	private Instance _instance;
	
	void setInstance(Instance instance) {
		this._instance = instance;
	}
    
    public final Result<Job,JsonObject> submitJob(JsonObject jco) throws IOException {
    	return _instance.connection().submitJob(this, jco);
    }
    
    
    Instance instance() {
    	return _instance;
    }
    
}
