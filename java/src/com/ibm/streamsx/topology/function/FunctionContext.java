/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function;

import java.net.MalformedURLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Context for a function executing in a IBM Streams application.
 */
public interface FunctionContext {
    
    /**
     * Get the container hosting a function 
     * @return Container hosting a function.
     */
    FunctionContainer getContainer();
    
    /**
     * Return a scheduler to execute background tasks. Functions should utilize
     * this service or {@link #getThreadFactory()} rather than creating their own threads
     * to ensure that the SPL runtime will wait for any background work
     * before completing an application.
     * <P>
     * The scheduler will be shutdown when the processing element is to be shutdown.
     * Once the scheduler is shutdown no new tasks will be accepted. Existing
     * scheduled tasks will remain in the scheduler's queue but periodic tasks
     * will canceled.
     * <BR>
     * Functions that implement {@code AutoCloseable} that wish to complete any outstanding tasks
     * at close time can call {@code ExecutorService.awaitTermination()}
     * to wait for outstanding tasks to complete or wait on the specific
     * {@code Future} reference for a task.
     * </P>
     * 
     * <P>
     * The returned scheduler service is guaranteed to be an instance
     * of {@code java.util.concurrent.ScheduledThreadPoolExecutor}
     * and initially has this configuration:
     * <UL>
     * <LI> {@code corePoolSize} Set to {@code Runtime.availableProcessors()} with
     * a minimum of 2 and maximum of 8. </LI>
     * <LI> {@code allowsCoreThreadTimeOut()} set to {@code true}</LI>
     * <LI> {@code keepAliveTime} set to 5 seconds</LI>
     * </UL>
     * Threads are created on demand to execute tasks, so that even if the
     * {@code corePoolSize} is eight, eight threads will only be created if
     * there are eight concurrent tasks scheduled. Threads will be removed
     * if they are not needed for the {@code keepAliveTime} value and
     * {@code allowsCoreThreadTimeOut()} returns {@code true}.
     * </P>
     * 
     * @return Scheduler service that can be used by the function.
     * @see "java.util.concurrent.ExecutorService"
     * @see "java.util.concurrent.Future"
     * @see "java.util.concurrent.ScheduledThreadPoolExecutor"
     */
    ScheduledExecutorService getScheduledExecutorService();
    
    /**
     * Return a ThreadFactory that can be used by the function with the thread context
     * class loader set correctly. Functions should utilize
     * the returned factory to create Threads.
     * <P>
     * Threads returned by the ThreadFactory have not been started
     * and are set as daemon threads. Functions may set the threads
     * as non-daemon before starting them. The SPL runtime will wait
     * for non-daemon threads before terminating a processing
     * element in standalone mode.
     * </P>
     * <P>
     * Any uncaught exception thrown by the {@code Runnable} passed
     * to the {@code ThreadFactory.newThread(Runnable)} will cause
     * the processing element containing the function to terminate.
     * </P>
     * <P>
     * The ThreadFactory will be shutdown when the processing element is to be shutdown.
     * Once the ThreadFactory
     * is shutdown a call to <code>newThread()</code> will return null.
     * </P>
     * 
     * @return A ThreadFactory that can be used by the function.
     */
    ThreadFactory getThreadFactory();
    
    /**
     * Get the index of the parallel channel the function is on.
     * <P>
     * If the function is in a parallel region, this method returns a value from
     * 0 to N-1, where N is the {@link #getMaxChannels() number of channels in the parallel region};
     * otherwise it returns -1.
     * </P>
     * 
     * @return the index of the parallel channel if the
     *         function is executing in a parallel region, or -1 if the function
     *         is not executing in a parallel region.
     * 
     */
    int getChannel();

    /**
     * Get the total number of parallel channels for the parallel region that
     * the function is in. If the function is not in a parallel region, this
     * method returns 0.
     * 
     * @return the number of parallel channels for the parallel region that this
     *         function is in, or 0 if the function is not in a parallel region.
     */
    int getMaxChannels();
    
    /**
     * Add class libraries to the functional class loader. The functional
     * class loader is set as the thread context class loader for
     * {@link #getThreadFactory() thread factory},
     * {@link #getScheduledExecutorService() executor} and any method
     * invocation on the function instance.
     * <P>
     * Functions use this method to add class libraries specific
     * to the invocation in
     * a consistent manner. An example is defining the jar files that
     * contain the JDBC driver to be used by the application.
     * <P>
     * Each element of {@code libraries} is trimmed and then converted 
     * into a {@code java.net.URL}. If the element cannot be converted
     * to a {@code URL} then it is assumed to represent a file system
     * path and is converted into an {@code URL} representing that path.
     * If the file path is relative the used location is currently
     * undefined, thus use of relative paths are not recommended.
     * <BR>
     * If a file path ends with {@code /* } then it is assumed to
     * be a directory and all jar files in the directory
     * with the extension {@code .jar} or {@code .JAR} are
     * added to the function class loader.
     * </P>
     * 
     * @param libraries String representations of URLs and file paths to be
     * added into the functional class loader. If {@code null} then no libraries
     * are added to the class loader.
     * 
     */
    void addClassLibraries(String[] libraries) throws MalformedURLException;
}
