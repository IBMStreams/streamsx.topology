/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.tester;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StandaloneTesterContextFuture<T> implements Future<T> {

        private final Future<T> topologyFuture;
        private final TesterRuntime trt;

        public StandaloneTesterContextFuture(Future<T> topologyFuture, TesterRuntime trt) {
            this.topologyFuture = topologyFuture;
            this.trt = trt;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean canceled = topologyFuture.cancel(mayInterruptIfRunning);
            if (canceled)
                shutdownTester();
            return canceled;
        }
        
        void shutdownTester() {
            try {
                trt.shutdown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isCancelled() {
            return topologyFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return topologyFuture.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            try {
                return topologyFuture.get();
            } finally {
                shutdownTester();
            }
        }

        @Override
        public T get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException,
                TimeoutException {
            boolean shutdownTester = true;
            try {
                return topologyFuture.get(timeout, unit);
            } catch (TimeoutException e) {
                shutdownTester = false;
                throw e;
            } finally {
                if (shutdownTester)
                    shutdownTester();
            }
        }

}
