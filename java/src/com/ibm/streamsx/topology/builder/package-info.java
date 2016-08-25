/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
/**
 * Low-level graph builder.
 * {@link com.ibm.streamsx.topology.builder.GraphBuilder} provides a layer
 * on top of {@code com.ibm.streams.flow.OperatorGraph}
 * to define a directed graph of connected operators.
 * Where possible information is maintained in
 * the graph declared by {@code OperatorGraph}
 * but this class maintains additional information
 * in order to allow SPL generation (through JSON).
 * <P>
 * <B><I>Note that the classes and methods in this
 * package is subject to change including the resultant
 * JSON object describing the graph.</I></B>
 * <BR>
 * The intended API for the Java Application API is the
 * functional API described by the package
 * {@code com.ibm.streamsx.topology}.
 * </P>
 */
package com.ibm.streamsx.topology.builder;