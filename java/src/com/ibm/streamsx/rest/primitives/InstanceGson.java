/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest.primitives;

import com.ibm.streamsx.rest.primitives.ActiveVersion;

/**
 * Class used to hold the Instance JSON structur
 */
class InstanceGson {

    public String activeServices;
    public ActiveVersion activeVersion;
    public String activeViews;
    public String configuredViews;
    public long creationTime;
    public String creationUser;
    public String domain;
    public String exportedStreams;
    public String health;
    public String hosts;
    public String id;
    public String importedStreams;
    public String jobs;
    public String operatorConnections;
    public String operators;
    public String owner;
    public String peConnections;
    public String pes;
    public String resourceAllocations;
    public String resourceType;
    public String restid;
    public String self;
    public long startTime;
    public String startedBy;
    public String status;
    public String views;
}
