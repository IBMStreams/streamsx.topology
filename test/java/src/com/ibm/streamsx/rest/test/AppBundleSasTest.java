/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
 */

package com.ibm.streamsx.rest.test;

import java.io.IOException;

import com.ibm.streamsx.rest.Instance;

/**
 * Test use of ApplicationBundle including Job submission
 * to a service.
 *
 */
public class AppBundleSasTest extends AppBundleTest {
	
	@Override
	protected Instance getInstance() throws IOException {
		return StreamingAnalyticsConnectionTest.getTestService().getInstance();
	}
}
