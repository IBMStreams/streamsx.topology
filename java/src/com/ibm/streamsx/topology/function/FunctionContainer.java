/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Container for a function executing in a IBM Streams application.
 */
public interface FunctionContainer {
    /**
     * Get the runtime identifier for this container's running job.
     * In distributed mode the identifier will be job identifier of
     * the submitted application.
     * When not in distributed mode the identifier is {@code 0}.
     * @return The job identifier for the current application.
     */
    String getJobId();
    
    /**
     * Return the name of this job.
     * @return name for the job the PE is running in
     */
    // String getJobName();

    /**
     * Get the runtime identifier for this container.
     * In distributed mode the identifier will be processing
     * element (PE) identifier of the process executing the function.
     * When not in distributed mode the identifier is {@code 0}.
     * @return The identifier for this container.
     */
    public String getId();
    
    /**
     * Return the IBM Streams domain identifier for this container.
     * In distributed mode the domain identifier will be that of
     * the IBM Streams domain running the application.
     * When not in distributed mode this will be the current operating
     * system user identifier.
     * @return Domain identifier for this container or the user identifier.
     */
    public String getDomainId();
    
    /**
     * Return the IBM Streams instance identifier for this container.
     * In distributed mode the instance identifier will be that of
     * the IBM Streams instance running the application.
     * When not in distributed mode this will be the current operating
     * system user identifier.
     * @return Instance identifier for this container or the user identifier.
     */
    public String getInstanceId();

    /**
     * Return the number of times this container has been relaunched.
     * For the first execution, the value will be 0.
     * @return number of times this container has been relaunched.
     */
    public int getRelaunchCount();
    
    /**
     * Get the host this container is executing on.
     * <P>
     * When running in distributed mode this returns the {@code InetAddress}
     * for the interface configured for application use by the
     * IBM Streams instance. This may differ from
     * from {@code java.net.InetAddress.getLocalHost()} if
     * there are multiple network interfaces on this host.
     * </P>
     * <P>
     * When not running in distributed, this
     * returns {@code java.net.InetAddress.getLocalHost()}.
     * </P>
     * @return Host this container is executing on
     */
    public InetAddress getConfiguredHost() throws UnknownHostException;
}
