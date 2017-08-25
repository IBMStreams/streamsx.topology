/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.List;

/**
 * The initial interface for interacting with the REST API of Streams.
 * Concrete instances are created with the factory methods in
 * {@link StreamsConnectionFactory}
 */
public interface StreamsConnection {

    /**
     * Gets a list of {@link Instance instances} that are available to this IBM
     * Streams connection
     * 
     * @return List of {@link Instance IBM Streams Instances} available to this
     *         connection
     * @throws IOException
     */
    List<Instance> getInstances() throws IOException;

    /**
     * Gets a specific {@link Instance instance} identified by the instanceId at
     * this IBM Streams connection
     * 
     * @param instanceId
     *            name of the instance to be retrieved
     * @return a single {@link Instance}
     * @throws IOException
     */
    Instance getInstance(String instanceId) throws IOException;

    /**
     * Cancels a job at this streams connection identified by the jobId
     * FIXME: Does this really belong at this level or should it be in Instance
     * or even Job? I think the current implementation only works on the default
     * domain/instance
     * 
     * @param jobId
     *            string identifying the job to be cancelled
     * @return a boolean indicating
     *         <ul>
     *         <li>true if the jobId is cancelled</li>
     *         <li>false if the jobId did not get cancelled</li>
     *         </ul>
     * @throws Exception
     */
    boolean cancelJob(String jobId) throws Exception;

}