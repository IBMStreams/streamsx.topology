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
 * 
 */
package com.ibm.streamsx.topology.builder;