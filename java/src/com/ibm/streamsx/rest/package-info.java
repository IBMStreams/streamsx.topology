/*
 * # Licensed Materials - Property of IBM # Copyright IBM Corp. 2017
 */

/**
 * Java wrappers for the REST API of IBM Streams.
 * 
 * <p>
 * This API is used to access the REST API in the IBM Streams product and the
 * IBM Streaming Analytics service through a java class.
 * </p>
 * 
 * <p>
 * For the IBM Streams product, a {@link com.ibm.streamsx.rest.StreamsConnection
 * Streams Connection} object is created to access the rest of the resourceTypes
 * in the instance.
 * </p>
 * 
 * <p>
 * For the IBM Streaming Analytics service, a
 * {@link com.ibm.streamsx.rest.StreamingAnalyticsConnection Streaming Analytics
 * Connection} object is created to access the rest of the resourceTypes in the
 * service.
 * </p>
 * 
 * <p>
 * In both cases, once the connection object is established, an
 * {@link com.ibm.streamsx.rest.Instance Instance} object is retrieved, either
 * directly via instance name, or as a list of all instances accessible by the
 * connection.
 * </p>
 * 
 * <p>
 * From the instance, all jobs in that instance are accessible. Each object
 * takes you a step further down the resource tree. Take a look at the
 * individual objects to see what is available as accessors, as this may change
 * over time.
 * </p>
 */

package com.ibm.streamsx.rest;

