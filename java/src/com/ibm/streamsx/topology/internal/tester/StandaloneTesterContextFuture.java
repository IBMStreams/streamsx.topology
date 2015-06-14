package com.ibm.streamsx.topology.internal.tester;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StandaloneTesterContextFuture<T> implements Future<T> {

        private final Future<T> topologyFuture;
        private final AutoCloseable tester;

        public StandaloneTesterContextFuture(Future<T> topologyFuture, AutoCloseable tester) {
            this.topologyFuture = topologyFuture;
            this.tester = tester;
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
                tester.close();
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
