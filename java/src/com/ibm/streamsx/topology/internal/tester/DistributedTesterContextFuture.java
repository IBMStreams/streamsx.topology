/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.tester;

import java.math.BigInteger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.ibm.streamsx.topology.internal.streams.InvokeCancel;

/**
 * A distributed job runs forever, but this allows
 * the testing code to cancel it.
 *
 */
public class DistributedTesterContextFuture implements Future<BigInteger> {

    private final BigInteger jobId;
    private final TesterRuntime trt;
    private boolean cancelled;

    public DistributedTesterContextFuture(BigInteger jobId, TesterRuntime trt) {
        this.jobId = jobId;
        this.trt = trt;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (isCancelled())
            return false;

        if (!mayInterruptIfRunning)
            return false;

        InvokeCancel cancel = new InvokeCancel(jobId);
        synchronized (this) {
            cancelled = true;
        }
        try {
            cancel.invoke(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public synchronized boolean isCancelled() {
        return cancelled;
    }

    @Override
    public synchronized boolean isDone() {
        return isCancelled();
    }
    
    @Override
    public synchronized BigInteger get() throws InterruptedException,
            ExecutionException {
        while (!cancelled)
            wait();
        return jobId;
    }

    @Override
    public synchronized BigInteger get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {

            while (!isCancelled()) {
                wait(unit.toMillis(timeout));
                if (!isCancelled()) {
                    throw new TimeoutException();
                }
            }
            return jobId;
    }

    void shutdownTester() {
        try {
            trt.shutdown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
