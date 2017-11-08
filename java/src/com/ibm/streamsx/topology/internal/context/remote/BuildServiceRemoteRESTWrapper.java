/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;

import java.io.File;
import java.io.IOException;
import org.apache.http.client.ClientProtocolException;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.StreamingAnalyticsService;
import com.ibm.streamsx.rest.StreamsRestFactory;

class BuildServiceRemoteRESTWrapper {
	
    private JsonObject service;
	
	BuildServiceRemoteRESTWrapper(JsonObject service){
	    this.service = service;
    }
	

	void remoteBuildAndSubmit(JsonObject submission, File archive) throws ClientProtocolException, IOException {
	    StreamingAnalyticsService sas = StreamsRestFactory.createStreamingAnalyticsService(service);
	    sas.buildAndSubmitJob(archive, submission);
	}
}

