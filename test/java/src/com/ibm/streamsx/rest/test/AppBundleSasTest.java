/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
 */

package com.ibm.streamsx.rest.test;

import static org.junit.Assume.assumeNotNull;

import java.io.IOException;

import org.junit.Before;

import com.ibm.streamsx.rest.Instance;

/**
 * Test use of ApplicationBundle including Job submission
 * to a service.
 *
 */
public class AppBundleSasTest extends AppBundleTest {
    
    @Before
    public void checkForService() {
        // if we don't have serviceName or vcapServices, skip the test
        assumeNotNull(System.getenv("STREAMING_ANALYTICS_SERVICE_NAME"), System.getenv("VCAP_SERVICES"));
    }
	
	@Override
	protected Instance getInstance() throws IOException {
		return StreamingAnalyticsConnectionTest.getTestService().getInstance();
	}
}
