/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018  
 */
package com.ibm.streamsx.topology.context;

/**
 * Result properties.
 * 
 * Properties that contain values as a result
 * of the {@link StreamsContext#submit(com.ibm.streamsx.topology.Topology, java.util.Map) submission}
 * of a topology.
 * <P>
 * Result properties are optionally placed into the configuration map passed into
 * {@link StreamsContext#submit(com.ibm.streamsx.topology.Topology, java.util.Map)}.
 * Each property must be individually requested by adding its key into the configuration map
 * with any value. Upon return the value will be replaced by value obtained during submission.
 * In some contexts the value may not be available.
 * </P>
 */
public interface ResultProperties {
    
	/**
	 * Result of a successful job submission as a JSON object.
	 * 
	 * For contexts that use a REST api the value is a
	 * {@code com.google.gson.JsonObject} holding these values:
	 * <UL>
	 * <LI>The JSON response from the HTTP REST request to submit the job.</LI>
	 * <LI>{@code buildStatus} - JSON object response from the HTTP REST request to build the topology.</LI>
	 * <LI>{@code submitMetrics} - JSON object containing metrics related to build service and job submission.</LI>
	 * <LI>{@code console.application.url} - Streams console URL for the instance the job was submitted to.</LI>
     * <LI>{@code console.application.job.url} - Streams console URL for the job.</LI>
	 * </UL>
	 * 
	 * Note that not all values may be produced, depending on context, and contents are subject to change.
	 * 
	 * <BR>
	 * For other contexts no object is produced.
	 *
	 * @since 1.11
	 */
    String JOB_SUBMISSION = "result.jobSubmission";
}
