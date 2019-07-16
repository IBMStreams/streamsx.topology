/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest.build;

import java.io.IOException;
import java.io.File;
import java.util.List;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Result;

/**
 * Access to a IBM Streams build service.
 * 
 * @since 1.12
 */
public interface BuildService {
	
	static BuildService of(String endpoint, String bearerToken) {
		return new StreamsBuildService(endpoint, bearerToken);
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
