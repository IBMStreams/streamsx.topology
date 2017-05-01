/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

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

    private String architecture;
    private String buildVersion;
    private String editionName;
    private String fullProductVersion;
    private String minimumOSBaseVersion;
    private String minimumOSPatchVersion;
    private String minimumOSVersion;
    private String productName;
    private String productVersion;
}
