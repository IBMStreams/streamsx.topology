/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Test;

import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.StreamingAnalyticsConnection;
import com.ibm.streamsx.rest.StreamingAnalyticsService;

@SuppressWarnings("deprecation")
public class StreamingAnalyticsConnectionTest extends StreamsConnectionTest {
	
	static StreamingAnalyticsService getTestService() throws IOException {
        String serviceName = System.getenv("STREAMING_ANALYTICS_SERVICE_NAME");
        String vcapServices = System.getenv("VCAP_SERVICES");
        
        // if we don't have serviceName or vcapServices, skip the test
        assumeNotNull(serviceName, vcapServices);
        

        StreamingAnalyticsService service = StreamingAnalyticsService.of(null, null);
    	
        Instance instance = service.getInstance();
        
        // bail if streaming analytics instance isn't up & running
        assumeTrue(instance.getStatus().equals("running"));
        
        return service;
	}

    @Override
    protected void setupConnection() throws Exception {
    	fail("Should not be called!");
    }

    @Override
    public void setupInstance() throws Exception {
        if (instance == null) {
        	
            testType = "STREAMING_ANALYTICS_SERVICE";
            
            instance = getTestService().getInstance();
        }
    }

    @Test
    public void testVCAPOptions() throws Exception {
        String serviceName = System.getenv("STREAMING_ANALYTICS_SERVICE_NAME");
        String existingVCAP = System.getenv("VCAP_SERVICES");
        String vcapString;
        List<Instance> instances;

        // VCAP_SERVICES can be either a string representing a json object, a
        // string representing a file or a JSON object
        // Let's check what we have and manipulate it to the other to confirm
        // the underlying functionality to decode VCAP_SERVICES is correct.

        // let's make sure we have something to work with
        System.out.println("Checking VCAP_SERVICES exists ...");
        assumeTrue(!existingVCAP.isEmpty());

        // is this a file?
        if (Files.isRegularFile(Paths.get(existingVCAP))) {
            System.out.println("convert file to json string ...");
            // we have a file, let's convert to a String and re-try
            vcapString = new String(Files.readAllBytes(Paths.get(existingVCAP)), StandardCharsets.UTF_8);

            StreamingAnalyticsConnection stringConn = StreamingAnalyticsConnection.createInstance(vcapString,
                    serviceName);
            instances = stringConn.getInstances();
            assertEquals(1, instances.size());

        } else {
            // let's convert to a file and try that
            System.out.println("convert json string to file ...");
            Path tempPath = Files.createTempFile("vCapTest", ".json");
            File tempFile = tempPath.toFile();

            Files.write(tempPath, existingVCAP.getBytes(StandardCharsets.UTF_8));
            tempFile.deleteOnExit();

            StreamingAnalyticsConnection fileConn = StreamingAnalyticsConnection.createInstance(tempPath.toString(),
                    serviceName);

            instances = fileConn.getInstances();
            assertEquals(1, instances.size());

            vcapString = existingVCAP; // need this setup later
        }

        // let's try a fake service name
        System.out.println("Try a non-existant service name ...");
        try {
            @SuppressWarnings("unused")
            StreamingAnalyticsConnection stringConn = StreamingAnalyticsConnection.createInstance(vcapString,
                    "FakeServiceName");
            fail("Worked with non-existant service name!");
        } catch (IllegalStateException e) {
            // should trigger an IllegalStateException exception here
            System.err.println("Expecting an exception here - FakeServiceName");
            e.printStackTrace();
        }

        // let's try a non-existant file
        System.out.println("Try a non-existant file ...");
        Path tempPath = Paths.get("nonExistantFileName");
        File tempFile = tempPath.toFile();
        tempFile.deleteOnExit();

        if (Files.isRegularFile(Paths.get(tempPath.toString()))) {
            fail("Non-existant file exists!!");
        }

        try {
            @SuppressWarnings("unused")
            StreamingAnalyticsConnection fileConn = StreamingAnalyticsConnection.createInstance(tempPath.toString(),
                    serviceName);
            fail("Worked with non-existant file!");
        } catch (IllegalStateException e) {
            // should trigger an IllegalStateException exception here
            // as the file isn't a file, and we'll throw when trying to act on a json string
            System.err.println("Expecting an exception here - Non-existant file");
            e.printStackTrace();
        }

        // let's try bad json
        String badJson = "{ extraCharacters, " + vcapString;

        // write junk to the file
        Files.createFile(tempPath) ;
        Files.write(tempPath, badJson.getBytes(StandardCharsets.UTF_8));

        if (!Files.isRegularFile(Paths.get(tempPath.toString()))) {
            fail("Could not write to tempFile!!");
        }

        try {
            @SuppressWarnings("unused")
            StreamingAnalyticsConnection fileConn = StreamingAnalyticsConnection.createInstance(tempPath.toString(),
                    serviceName);
            fail("Worked with bad json!");
        } catch (com.google.gson.JsonSyntaxException e) {
            // should trigger an exception here
            System.err.println("Expecting an exception here - Junk filled file");
            e.printStackTrace();
        }

        // use bad json as a string
        try {
            @SuppressWarnings("unused")
            StreamingAnalyticsConnection fileConn = StreamingAnalyticsConnection.createInstance(badJson, serviceName);
            fail("Worked with bad json as string!");
        } catch (com.google.gson.JsonSyntaxException e) {
            // should trigger an exception here
            System.err.println("Expecting an exception here - Bad Json");
            e.printStackTrace();
        }

        // currently we don't test for a JSON object to create the
        // StreamingAnalyticsConnection object as that's not supported yet.

    }
}
