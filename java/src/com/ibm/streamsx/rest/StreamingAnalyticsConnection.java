/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;

/**
 * The initial interface for interacting with the REST API of a Streams instance
 * on the Streaming Analytics Service on IBM Bluemix. Since Streaming Analytics
 * Service has a single Streams instance (at least for now), this extends
 * {@link StreamsConnection} and adds a convenience method {@link getInstance}
 * to get the single instance.
 * <p>
 * Concrete instances are created with the factory methods in
 * {@link StreamsRestFactory}
 */
public interface StreamingAnalyticsConnection extends StreamsConnection {
    /**
     * Gets the {@link Instance IBM Streams Instance} object for the Streaming
     * Analytics service.
     *
     * @return an {@link Instance IBM Streams Instance} associated with this
     *         connection
     *
     * @throws IOException
     */
    Instance getInstance() throws IOException;
}
