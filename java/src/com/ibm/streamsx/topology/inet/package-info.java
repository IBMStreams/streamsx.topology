/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
/**
 * Network access to streaming data.
 * <P>
 * Functionality in this package is provided
 * by the open source SPL toolkit
 * <a href="http://ibmstreams.github.io/streamsx.inet/">com.ibm.streamsx.inet</a>.
 * This requires that the {@code com.ibm.streamsx.inet} is made available
 * when submitting the application, by:
 * <UL>
 * <LI>Including the toolkit in the toolkit path specified by the environment variable {@code STREAMS_SPLPATH}</LI>
 * <LI>Specifically adding the toolkit using {@link com.ibm.streamsx.topology.spl.SPL#addToolkit(com.ibm.streamsx.topology.TopologyElement, java.io.File)}. </LI>
 * <LI>(Future) - Using a release of IBM Streams that includes a suitable version.</LI>
 * </UL>
 * Currently a more recent version of the toolkit is required than the one supplied IBM Streams (2.0).
 * </P>
 */
package com.ibm.streamsx.topology.inet;