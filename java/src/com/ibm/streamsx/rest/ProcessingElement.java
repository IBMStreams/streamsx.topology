/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.annotations.Expose;

/**
 * An object describing an IBM Streams Processing Element
 *
 */
public class ProcessingElement extends Element {

    @Expose
    private String connections;
    @Expose
    private String currentWorkingPath;
    @Expose
    private String domain;
    @Expose
    private String health;
    @Expose
    private String host;
    @Expose
    private String id;
    @Expose
    private long indexWithinJob;
    @Expose
    private String inputPorts;
    @Expose
    private String instance;
    @Expose
    private String job;
    @Expose
    private int launchCount;
    @Expose
    private String metrics;
    @Expose
    private String operators;
    @Expose
    private String optionalConnections;
    @Expose
    private ArrayList<String> osCapabilities;
    @Expose
    private String outputPorts;
    @Expose
    private String pendingTracingLevel;
    @Expose
    private String processId;
    @Expose
    private boolean relocatable;
    @Expose
    private String requiredConnections;
    @Expose
    private String resourceAllocation;
    @Expose
    private ArrayList<String> resourceTags;
    @Expose
    private String resourceType;
    @Expose
    private boolean restartable;
    @Expose
    private String restid;
    @Expose
    private String status;
    @Expose
    private String statusReason;
    @Expose
    private String tracingLevel;

    final static List<ProcessingElement> createPEList(AbstractStreamsConnection sc,
            String pes) throws IOException {
        return createList(sc, pes, ProcessingElementArray.class);
    }

    /**
     * Gets a list of {@link Metric metrics} for this processing element
     * 
     * @return List of {@link Metric IBM Streams Metrics}
     * @throws IOException
     */
    public List<Metric> getMetrics() throws IOException {
        List<Metric> lMetrics = Metric.getMetricList(connection(), metrics);
        return lMetrics;
    }

    /**
     * Gets a list of {@link PEInputPort processing element input ports} for
     * this processing element
     * 
     * @return List of {@link PEInputPort Processing Element Input Ports}
     * @throws IOException
     */
    public List<PEInputPort> getInputPorts() throws IOException {
        return PEInputPort.createInputPortList(connection(), inputPorts);
    }

    /**
     * Gets a list of {@link Operator operators} for this processing element
     * 
     * @return List of {@link Operator IBM Streams Operators}
     * @throws IOException
     */
    public List<Operator> getOperators() throws IOException {
       return Operator.createOperatorList(connection(), operators);
    }

    /**
     * Gets a list of {@link PEOutputPort processing element output ports} for
     * this processing element
     * 
     * @return List of {@link PEOutputPort Processing Element Output Ports}
     * @throws IOException
     */
    public List<PEOutputPort> getOutputPorts() throws IOException {
        return PEOutputPort.createOutputPortList(connection(), outputPorts);
    }

    /**
     * Gets the current working path of the processing element
     * 
     * @return the current working path
     */
    public String getCurrentWorkingPath() {
        return currentWorkingPath;
    }

    /**
     * Gets the health indicator for this processing element
     * 
     * @return the health indicator that contains one of the following values:
     *         <ul>
     *         <li>healthy</li>
     *         <li>partiallyHealthy</li>
     *         <li>partiallyUnhealthy</li>
     *         <li>unhealthy</li>
     *         <li>unknown</li>
     *         </ul>
     */
    public String getHealth() {
        return health;
    }

    /**
     * Gets the id of this processing element
     * 
     * @return the processing element id
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the index of this processing element within the {@link Job}
     * 
     * @return processing element index as a long
     */
    public long getIndexWithinJob() {
        return indexWithinJob;
    }

    /**
     * Gets the number of times this processing element was started manually or
     * automatically because of failures
     * 
     * @return number of times the processing element was started as an int
     */
    public int getLaunchCount() {
        return launchCount;
    }

    /**
     * Gets the status of optional connections for this processing element.
     * 
     * @return the optional connection status that contains one of the following
     *         values:
     *         <ul>
     *         <li>connected</li>
     *         <li>disconnected</li>
     *         <li>partiallyConnected</li>
     *         <li>unknown</li>
     *         </ul>
     */
    public String getOptionalConnections() {
        return optionalConnections;
    }

    /**
     * Gets a list of the operating system capabilities
     * 
     * @return List of the operating system capabilities
     */
    public List<String> getOsCapabilities() {
        return osCapabilities;
    }

    /**
     * Describes a pending change to the granularity of the trace information
     * that is stored for this processing element.
     * 
     * @return the pending trace level change that contains one of the following
     *         values:
     *         <ul>
     *         <li>off</li>
     *         <li>debug</li>
     *         <li>error</li>
     *         <li>trace</li>
     *         </ul>
     *         a null value indicates no pending change to the trace level
     */
    public String getPendingTracingLevel() {
        return pendingTracingLevel;
    }

    /**
     * Gets the operating system process ID for this processing element
     * 
     * @return the operating sytem process ID
     */
    public String getProcessId() {
        return processId;
    }

    /**
     * Indicates whether or not this processing element can be relocated to a
     * different resource
     * 
     * @return boolean indicating whether or not relocation is possible
     */
    public boolean getRelocatable() {
        return relocatable;
    }

    /**
     * Status of the required connections for this processing element.
     * 
     * @return required connection status that contains one of the following
     *         values:
     *         <ul>
     *         <li>connected</li>
     *         <li>disconnected</li>
     *         <li>partiallyConnected</li>
     *         <li>unknown</li>
     *         </ul>
     */
    public String getRequiredConnections() {
        return requiredConnections;
    }

    /**
     * Gets a list of resource tags for this processing element
     * 
     * @return List of resource tags
     */
    public List<String> getResourceTags() {
        return resourceTags;
    }

    /**
     * Identifies the REST resource type
     * 
     * @return "pe"
     */
    public String getResourceType() {
        return resourceType;
    }

    /**
     * Indicates whether or not this processing element can be restarted
     * 
     * @return the restart indicator as a boolean
     */
    public boolean getRestartable() {
        return restartable;
    }

    /**
     * Gets the status of this processing element
     * 
     * @return the processing element status
     */
    public String getStatus() {
        return status;
    }

    /**
     * Gets additional status for this processing element
     * 
     * @return any addition status for this processing element
     */
    public String getStatusReason() {
        return statusReason;
    }

    /**
     * Gets the granularity of the tracing level for this processing element
     * 
     * @return the current tracing level that contains one of the following
     *         values:
     *         <ul>
     *         <li>off</li>
     *         <li>debug</li>
     *         <li>error</li>
     *         <li>trace</li>
     *         </ul>
     */
    public String getTracingLevel() {
        return tracingLevel;
    }
    
    /**
     * Gets the {@link ResourceAllocation resource allocation} for this
     * processing element.
     * 
     * @return {@link ResourceAllocation resource allocation}
     * @throws IOException
     * 
     * @since 1.9
     */
    public ResourceAllocation getResourceAllocation() throws IOException {
        return create(connection(), resourceAllocation, ResourceAllocation.class);
    }

    /**
     * internal usage to get the list of processing elements
     * 
     */
    private static class ProcessingElementArray extends ElementArray<ProcessingElement> {
        @Expose
        private ArrayList<ProcessingElement> pes;
        List<ProcessingElement> elements() { return pes;}
    }
}
