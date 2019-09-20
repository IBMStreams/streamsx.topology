/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest.build;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.IOException;
import java.net.URL;
import java.io.File;
import java.util.List;
import java.util.function.Function;

import org.apache.http.client.fluent.Executor;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Result;
import com.ibm.streamsx.rest.internal.ICP4DAuthenticator;
import com.ibm.streamsx.rest.internal.RestUtils;
import com.ibm.streamsx.rest.internal.StandaloneAuthenticator;
import com.ibm.streamsx.topology.internal.streams.Util;

/**
 * Access to a IBM Streams build service.
 * 
 * @since 1.12
 */
public interface BuildService {
	
	public static final String STREAMS_REST_RESOURCES = "/streams/rest/resources";

    public static BuildService ofEndpoint(String endpoint, String name, String userName, String password,
            boolean verify) throws IOException {
	    if (name == null && System.getenv(Util.STREAMS_INSTANCE_ID) == null) {
	        // Standalone. Use endpoint from env if unset, and add known path
            if (endpoint == null) {
                endpoint = Util.getenv(Util.STREAMS_BUILD_URL);
            }
            if (!endpoint.endsWith(STREAMS_REST_RESOURCES)) {
                URL url = new URL(endpoint);
                URL resourcesUrl = new URL(url.getProtocol(), url.getHost(),
                        url.getPort(), STREAMS_REST_RESOURCES);
                endpoint = resourcesUrl.toExternalForm();
            }
	        StandaloneAuthenticator auth = StandaloneAuthenticator.of(endpoint, userName, password);
	        JsonObject serviceDefinition = auth.config(verify);
	        if (serviceDefinition == null) {
	            // Problem with security service, fall back to basic auth, so
	            // user and password are required.
	            if (userName == null || password == null) {
	                String[] values = Util.getDefaultUserPassword(userName, password);
	                userName = values[0];
	                password = values[1];
	            }
	            String basicAuth = RestUtils.createBasicAuth(userName, password);
	            return StreamsBuildService.of(e -> basicAuth, endpoint, verify);
	        }
	        return StreamsBuildService.of(auth, serviceDefinition, verify);
	    } else {
	        ICP4DAuthenticator auth = ICP4DAuthenticator.of(endpoint, name, userName, password);
	        return StreamsBuildService.of(auth, auth.config(verify), verify);
	    }
	}

    public static BuildService ofServiceDefinition(JsonObject serviceDefinition, boolean verify) throws IOException {
        String name = jstring(serviceDefinition, "service_name");
        Function<Executor,String> authenticator;
        if (name == null) {
            authenticator = StandaloneAuthenticator.of(serviceDefinition);
        } else {
            authenticator = ICP4DAuthenticator.of(serviceDefinition);
        }

        return StreamsBuildService.of(authenticator, serviceDefinition, verify);
    }
	
	void allowInsecureHosts();

	/**
	 * Submit an archive to build using the IBM Streams build service.
	 * <P>
	 * The returned {@link Result} instance has:
	 * <UL>
	 * <LI>{@link Result#getId()} returning the job identifier or {@code null} if a
	 * job was not created..</LI>
	 * <LI>{@link Result#getElement()} returning a {@link Job} instance for the
	 * submitted job or {@code null} if a job was not created.</LI>
	 * <LI>{@link Result#getRawResult()} return the raw JSON response.</LI>
	 * </UL>
	 * </P>
	 * 
	 * @param archive     The application archive to build.
	 * @param buildName   A name for the build, or null.
	 * @param buildConfig Build configuration, or null.
	 * @return Result of the build and job submission.
	 * @throws IOException Error communicating with the service.
	 * 
	 */
	Build createBuild(String buildName, JsonObject buildConfig)
			throws IOException;	


	/**
	 * Gets a list of {@link Toolkit toolkits} installed on the IBM Streams 
	 * build service.
	 * 
	 * @since 1.13
	 */
	public List<Toolkit> getToolkits() throws IOException;

	/**
	 * Gets a {@link Toolkit toolkit} installed on the IBM Streams build 
	 * service.
	 * 
	 * @param toolkitId The ID of the toolkit to retrieve.
	 * @return The toolkit, or null if no matching toolkit was found.
	 * @since 1.13
	 */
	public Toolkit getToolkit(String toolkitId) throws IOException;

	/**
	 * Install a toolkit in the build service from a local path.  The path
	 * must be a directory containing a single toolkit.  If the toolkit's
	 * name and version exactly match those of a toolkit already in the build
	 * service, the existing toolkit will not be replaced.
	 *
	 * @return A {@link Toolkit} object representing the newly installed toolkit,
	 * or null if the toolkit was not installed.
	 * @throws IOException
	 */
	public Toolkit uploadToolkit(File path) throws IOException;
}
