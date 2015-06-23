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
import java.util.logging.Level;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.topology.context.ContextProperties;

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
        Level traceLevel = (Level) config
                .get(ContextProperties.TRACING_LEVEL);
        if (traceLevel != null) {
            commands.add("-t");

            // -t, --trace-level=INT Trace level: 0 - OFF, 1 - ERROR, 2 - WARN,
            // 3 - INFO, 4 - DEBUG, 5 - TRACE.
            
            int tli = traceLevel.intValue();
            String tls;
            if (tli == Level.OFF.intValue())
                tls = "0";
            else if (tli == Level.ALL.intValue())
                tls = "5";
            else if (tli >= TraceLevel.ERROR.intValue())
                tls = "1";
            else if (tli >= TraceLevel.WARN.intValue())
                tls = "2";
            else if (tli >= TraceLevel.INFO.intValue())
                tls = "3";
            else if (tli >= TraceLevel.DEBUG.intValue())
                tls = "4";
            else
                tls = "5";
            commands.add(tls);
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
