/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import com.google.gson.annotations.Expose;

/**
 * Basic class to hold the ActiveVersion JSON structure
 */
public class ActiveVersion {

    public String getArchitecture() {
        return architecture;
    }

    public String getBuildVersion() {
        return buildVersion;
    }

    public String getEditionName() {
        return editionName;
    }

    public String getFullProductVersion() {
        return fullProductVersion;
    }

    public String getMinimumOSBaseVersion() {
        return minimumOSBaseVersion;
    }

    public String getMinimumOSPatchVersion() {
        return minimumOSPatchVersion;
    }

    public String getMinimumOSVersion() {
        return minimumOSVersion;
    }

    public String getProductName() {
        return productName;
    }

    public String getProductVersion() {
        return productVersion;
    }

    @Expose
    private String architecture;
    @Expose
    private String buildVersion;
    @Expose
    private String editionName;
    @Expose
    private String fullProductVersion;
    @Expose
    private String minimumOSBaseVersion;
    @Expose
    private String minimumOSPatchVersion;
    @Expose
    private String minimumOSVersion;
    @Expose
    private String productName;
    @Expose
    private String productVersion;
}
