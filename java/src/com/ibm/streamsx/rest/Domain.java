/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import com.google.gson.annotations.Expose;

/**
 * 
 * An object describing an IBM Streams Domain
 * 
 * @since 1.8
 */
public class Domain extends Element {
    
    @Expose
    private String id;
    @Expose
    private String status;
    @Expose
    private long creationTime;
    @Expose
    private String creationUser;
    @Expose
    private String zooKeeperConnectionString;

    /**
     * Gets the time in milliseconds when this domain was created.
     * 
     * @return the epoch time in milliseconds when the domain was created.
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Gets the user ID that created this instance.
     * 
     * @return the creation user ID
     */
    public String getCreationUser() {
        return creationUser;
    }

    /**
     * Gets the IBM Streams unique identifier for this domain.
     * 
     * @return the IBM Streams unique identifier.
     */
    public String getId() {
        return id;
    }


    /**
     * Gets the status of the domain.
     *
     * @return the instance status that contains one of the following values:
     *         <ul>
     *         <li>running</li>
     *         <li>stopping</li>
     *         <li>stopped</li>
     *         <li>starting</li>
     *         <li>removing</li>
     *         <li>unknown</li>
     *         </ul>
     * 
     */
    public String getStatus() {
        return status;
    }
    

    /**
     * Gets the ZooKeeper connection string for this domain.
     * @return ZooKeeper connection string for this domain.
     */
    public String getZooKeeperConnectionString() {
        return zooKeeperConnectionString;
    }
}
