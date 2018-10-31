/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
*/
package com.ibm.streamsx.rest;

import com.google.gson.annotations.Expose;

class UploadedApplicationBundle extends ApplicationBundle {
	
    @Expose
    private String bundleId;
    
    String getBundleId() {
        return bundleId;
    }
    
    @Override
    public void refresh() {}
}
