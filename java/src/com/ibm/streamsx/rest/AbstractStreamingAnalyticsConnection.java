/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AUTH;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;

import com.google.gson.JsonObject;

/**
 * Common code for implementations of {@link IStreamingAnalyticsConnection}
 */
abstract class AbstractStreamingAnalyticsConnection 
        extends AbstractStreamsConnection {

    private static final Logger traceLog = Logger.getLogger("com.ibm.streamsx.rest.AbstractStreamingAnalyticsConnection");

    private final AbstractStreamingAnalyticsService service;
    private Instance instance;
    String baseConsoleURL;

    AbstractStreamingAnalyticsConnection(
    		AbstractStreamingAnalyticsService service,
            String resourcesUrl, boolean allowInsecure)
            {
        super(resourcesUrl, allowInsecure);
        this.service = service;
    }
    
    final JsonObject credentials() {
    	return service.credentials();
    }
    
    @Override
    final String getAuthorization() {
    	return service.getAuthorization();
    }

    /**
     * Gets the {@link Instance IBM Streams Instance} object for the Streaming
     * Analytics service.
     *
     * @return an {@link Instance IBM Streams Instance} associated with this
     *         connection
     *
     * @throws IOException
     */
    Instance getInstance() throws IOException {
        if (instance != null)
            return instance;
        List<Instance> instances = getInstances();
        if (instances.size() == 1) {
            // Should find one only
            instance = instances.get(0);
            instance.setApplicationConsoleURL(baseConsoleURL);
            return instance;
        } else {
            throw new RESTException("Unexpected number of instances: " + instances.size());
        }
    }

    protected boolean delete(String deleteJobUrl) throws IOException {
        boolean rc = false;
        String sReturn = "";

        Request request = Request
                .Delete(deleteJobUrl)
                .addHeader(AUTH.WWW_AUTH_RESP, getAuthorization())
                .useExpectContinue();

        Response response = executor.execute(request);
        HttpResponse hResponse = response.returnResponse();
        int rcResponse = hResponse.getStatusLine().getStatusCode();

        if (HttpStatus.SC_OK == rcResponse) {
            sReturn = EntityUtils.toString(hResponse.getEntity());
            rc = true;
        } else {
            rc = false;
        }
        traceLog.finest("Request: [" + deleteJobUrl + "]");
        traceLog.finest(rcResponse + ": " + sReturn);
        return rc;
    }

    @Override
    Result<Job, JsonObject> submitJob(ApplicationBundle bundle, JsonObject jco) throws IOException {    	
    	return service.submitJob(((FileBundle) bundle).bundleFile(), jco);
    }
}
