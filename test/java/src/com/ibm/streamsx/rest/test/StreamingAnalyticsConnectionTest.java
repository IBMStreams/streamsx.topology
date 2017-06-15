/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.StreamingAnalyticsConnection;

public class StreamingAnalyticsConnectionTest extends StreamsConnectionTest {

    // Tests that access StreamingAnalyticsConnection need to be overridden in
    // this class
    StreamingAnalyticsConnection connection = null;

    @Override
    public void setupConnection() {
        try {
            if (connection == null) {
                String serviceName = System.getenv("STREAMING_ANALYTICS_SERVICE_NAME");
                String vcapServices = System.getenv("VCAP_SERVICES");

                // if we don't have serviceName or vcapServices, skip the test
                assumeNotNull(serviceName, vcapServices);

                testType = "STREAMING_ANALYTICS_SERVICE";
                connection = StreamingAnalyticsConnection.createInstance(vcapServices, serviceName);
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Override
    public void setupInstance() {
        setupConnection();
        try {
            if (instance == null) {
                instance = connection.getInstance();
                // bail if streaming analytics instance isn't up & running
                assumeTrue(instance.getStatus().equals("running"));
            }
        } catch (RESTException r) {
            // if we get here, the local Streams test has failed
            r.printStackTrace();
            fail(r.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Override
    @Test
    public void testGetInstances() {
        setupConnection();
        try {
            // get all instances in the domain
            List<Instance> instances = connection.getInstances();
            // there should be at least one instance
            assertTrue(instances.size() > 0);

            String instanceName = instances.get(0).getId();
            Instance i2 = connection.getInstance(instanceName);
            assertEquals(instanceName, i2.getId());

        } catch (RESTException r) {
            r.printStackTrace();
            fail(r.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            // try a fake instance name
            connection.getInstance("fakeName");
            fail("the connection.getInstance() call should have thrown an exception");
        } catch (RESTException r) {
            assertEquals(404, r.getStatusCode());
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Override
    @Test
    public void testCancelSpecificJob() {
        try {
            if (jobId != null) {
                // cancel the job
                boolean cancel = connection.cancelJob(jobId);
                assertTrue(cancel == true);
                // remove these so @After doesn't fail
                job = null;
                jobId = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
