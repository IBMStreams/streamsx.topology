/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.streams;

import static com.ibm.streamsx.topology.internal.streams.InvokeSc.trace;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.ibm.streams.operator.logging.TraceLevel;

public class InvokeStandalone {

    private File bundle;

    public InvokeStandalone(File bundle) {
        super();
        this.bundle = bundle;
    }

    public Future<Integer> invoke(Map<String, ? extends Object> config)
            throws Exception, InterruptedException {
        String si = System.getProperty("java.home");
        File jvm = new File(si, "bin/java");

        List<String> commands = new ArrayList<String>();
        commands.add(jvm.getAbsolutePath());
        commands.add("-jar");
        commands.add(bundle.getAbsolutePath());
        TraceLevel traceLevel = (TraceLevel) config.get("streams.trace");
        if (traceLevel != null) {
            commands.add("-t");
            commands.add("4");
        }

        trace.info("Invoking standalone application");
        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.inheritIO();
        Process standaloneProcess = pb.start();

        return new ProcessFuture(standaloneProcess);

        /*
         * int rc = standaloneProcess.waitFor();
         * trace.info("Standalone application completed: return code=" + rc); if
         * (rc != 0) throw new Exception("Standalone application failed!");
         */
    }

    private static class ProcessFuture implements Future<Integer> {

        private final Process process;
        private int rc;
        private boolean isDone;
        private boolean isCancelled;

        ProcessFuture(Process process) {
            this.process = process;
        }

        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning) {
            if (isDone())
                return false;
            if (!mayInterruptIfRunning)
                return false;

            process.destroy();
            isCancelled = true;
            notifyAll();
            return true;
        }

        @Override
        public synchronized boolean isCancelled() {
            return isCancelled;
        }

        @Override
        public synchronized boolean isDone() {
            return isDone || isCancelled;
        }

        @Override
        public synchronized Integer get() throws InterruptedException,
                ExecutionException {
            if (isDone)
                return rc;
            rc = process.waitFor();
            notifyAll();
            return rc;
        }

        @Override
        public synchronized Integer get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException,
                TimeoutException {
            if (!isDone()) {
                wait(unit.toMillis(timeout));
            }
            if (isCancelled())
                throw new CancellationException();
            if (!isDone)
                throw new TimeoutException();
            return rc;
        }
    }
}
