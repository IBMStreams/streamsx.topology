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
 * An interface for using the REST API of IBM Clould Streaming Analytics Service.
 * Not all REST services are supported at this time, only the ones currently
 * used by Topology for build and job submission.
 * @since 1.8
 */
public interface StreamingAnalyticsService {
    /**
     * Submit a Streams bundle to run on the Streaming Analytics Service.
     * <p>The JSON object may contain an optional {@code deploy} member that
     * includes deployment information.
     * @param bundle A streams application bundle
     * @param submission Deployment info to be submitted.
     * @return The job id, or -1. Results from the submit will be added to the
     * submission parameter object as a member named @{code submissionResults}.
     * @throws IOException
     */
    BigInteger submitJob(File bundle, JsonObject submission) throws IOException;

    /**
     * Submit an archive to build on the Streaming Analytics Service, and submit
     * the job if the build is successful.
     * <p>
     * The JSON object contains two keys:
     * <UL>
     * <LI>{@code deploy} - Optional - Deployment information.</LI>
     * <LI>{@code graph} - Required - JSON representation of the topology graph.</LI>
     * </UL>
     * <p>
     * Results are added to the submission parameters object.
     * @param archive The application archive to build.
     * @param submission Topology and deployment info to be submitted.
     * @return The job id, or -1. Results from the submit will be added to the
     * submission parameter object as a member named @{code submissionResults}.
     * @throws IOException
     */
    BigInteger buildAndSubmitJob(File archive, JsonObject submission) throws IOException;
}
