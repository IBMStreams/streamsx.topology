/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;

import com.google.gson.JsonObject;

/**
 * An interface for using the REST API of Bluemix Streaming Analytics Service.
 * Not all REST services are supported at this time, only the ones currently
 * used by Topology around build and job submission.
 */
public interface StreamingAnalyticsService {
    /**
     * Submit a Streams bundle to run on the Streaming Analytics Service.
     * @param bundle A streams application bundle
     * @param submission Submission parameters, may have results added.
     * @return The job number. The submission results will also be added to the
     * submission parameters object.
     * @throws IOException
     */
    BigInteger submitJob(File bundle, JsonObject submission) throws IOException;

    /**
     * Submit an archive to build on the Streaming Analytics Service, and submit
     * the job if the build is successful.
     * <p>
     * Results are added to the submission parameters object.
     * @param archive The application archive to build.
     * @param submission Submission parameters, may have results added.
     * @throws IOException
     */
    void buildAndSubmitJob(File archive, JsonObject submission) throws IOException;
}
