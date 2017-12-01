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
 * {@link StreamsRestFactory}
 */
interface IStreamsConnection {

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
}