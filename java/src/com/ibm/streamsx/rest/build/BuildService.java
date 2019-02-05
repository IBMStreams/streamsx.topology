/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest.build;

import java.io.IOException;

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
}
