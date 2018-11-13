/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015 ,2018
 */

/**
 * Context for executing IBM Streams Java topologies.
 * <h3>Submission/Execution</h3>
 * There are four (primary) mechanisms to submit or execute a Java topology:
 * <UL>
 * <LI>{@link com.ibm.streamsx.topology.context.StreamsContext.Type#DISTRIBUTED} - The topology is compiled into
 * a Streams application bundle and submitted to a Streams instance.</LI>
 * <LI>{@link com.ibm.streamsx.topology.context.StreamsContext.Type#BUNDLE} - The topology is compiled into
 * a Streams application bundle which can then be submitted to a Streams instance.</LI>
 * <LI>{@link com.ibm.streamsx.topology.context.StreamsContext.Type#STANDALONE} - The topology is compiled into
 * a Streams application standalone bundle and executed as a separate child process of the Java virtual machine.</LI>
 * <LI>{@link com.ibm.streamsx.topology.context.StreamsContext.Type#EMBEDDED} -  The topology is executed using the Java Streams runtime,
 * embedded within the Java virtual machine.</LI>
 * </UL>
 * For a full list of the supported types of {@link com.ibm.streamsx.topology.context.StreamsContext}
 * see {@link com.ibm.streamsx.topology.context.StreamsContext.Type}.
 * <h3>SPL Compilation and Integration</h3>
 * When a Streams application bundle is produced, then the
 * SPL compiler {@code sc} is used to produce the bundle ({@code sab} file).
 * The toolkit path for compilation is:
 * <OL>
 * <LI>The generated application toolkit containing the generated SPL code for the topology.</LI>
 * <LI>The com.ibm.streamsx.topology toolkit (its location is auto-discovered).</LI>
 * <LI>Toolkits added by {@link com.ibm.streamsx.topology.spl.SPL#addToolkit(TopologyElement, File)} .</LI>
 * <LI>The value of the environment variable {@code STREAMS_SPLPATH} if it is set.</LI>
 * <LI>The toolkits from the Streams product: {@code $STREAMS_INSTALL/toolkits}.</LI>
 * </OL>
 */
package com.ibm.streamsx.topology.context;


