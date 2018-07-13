/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.streams;

import static com.ibm.streamsx.topology.internal.streams.InvokeSc.trace;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.jobconfig.JobConfig;
import com.ibm.streamsx.topology.jobconfig.SubmissionParameter;

public class InvokeStandalone {

    private final File bundle;
    // Keep bundle when finished.
    private final boolean keepBundle;
    private final Map<String,String> envVars = new HashMap<>();

    public InvokeStandalone(File bundle, boolean keepBundle) {
        super();
        this.bundle = bundle;
        this.keepBundle = keepBundle;
    }
    
    public void addEnvironmentVariable(String key, String value) {
        envVars.put(key, value);
    }

    public Future<Integer> invoke(JsonObject deploy)
            throws Exception, InterruptedException {
        String si = System.getProperty("java.home");
        File jvm = new File(si, "bin/java");

        JobConfig jc = JobConfigOverlay.fromFullOverlay(deploy);

        List<String> commands = new ArrayList<>();
        commands.add(jvm.getAbsolutePath());
        commands.add("-jar");
        commands.add(bundle.getAbsolutePath());
        String traceLevel = jc.getStreamsTracing();
        if (traceLevel != null) {
            commands.add("-t");

            // -t, --trace-level=INT Trace level: 0 - OFF, 1 - ERROR, 2 - WARN,
            // 3 - INFO, 4 - DEBUG, 5 - TRACE.
            final String tls;
            switch (traceLevel) {
                case "off": tls = "0"; break;
                default:
                case "error": tls = "1"; break;
                case "warn":  tls = "2"; break;
                case "info":  tls = "3"; break;
                case "debug": tls = "4"; break;
                case "trace": tls = "5"; break;
            }
            commands.add(tls);
        }
        if (jc.hasSubmissionParameters()) {
            for(SubmissionParameter param : jc.getSubmissionParameters()) {
                // note: this execution path does correctly
                // handle / preserve the semantics of escaped \t and \n.
                // e.g., "\\n" is NOT treated as a newline 
                // rather it's the two char '\','n'
                commands.add(param.getName()+"="+param.getValue());
            }
        }

	String datadir = jc.getDataDirectory();
	if (datadir != null) {
	    commands.add("--data-directory");
	    commands.add(datadir);
	}

        if (deploy.has("topology.standaloneRunTime")) {
             double rt = deploy.get("topology.standaloneRunTime").getAsDouble();
             commands.add("--kill-after=" + rt);
        }
        
        trace.info("Invoking standalone application");
        trace.info(Util.concatenate(commands));

        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.inheritIO();

        for (Entry<String,String> ev : envVars.entrySet()) {
            trace.fine("Setting environment variable for standalone: " + ev.getKey() + "=" + ev.getValue());
            pb.environment().put(ev.getKey(), ev.getValue());
        }

        Process standaloneProcess = pb.start();

        return new ProcessFuture(standaloneProcess, keepBundle ? null : bundle);
    }

    private static class ProcessFuture implements Future<Integer> {

        private final Process process;
        // Set if bundle to be deleted.
        private File bundle;
        private int rc;
        private boolean isDone;
        private boolean isCancelled;

        ProcessFuture(Process process, File bundle) {
            this.process = process;
            this.bundle = bundle;
            if (bundle != null)
                bundle.deleteOnExit();
        }
        
        private void deleteBundle() {
            if (bundle != null) {
                bundle.delete();
                bundle = null;
            }
        }

        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning) {
            if (isDone())
                return false;
            if (!mayInterruptIfRunning)
                return false;

            try {
                process.destroy();
            } finally {
                deleteBundle();
            }
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
            boolean done =  isDone || isCancelled;
            if (done)
                deleteBundle();
            return done;
        }

        @Override
        public synchronized Integer get() throws InterruptedException,
                ExecutionException {
            if (isDone())
                return rc;
            try {
                rc = process.waitFor();
            } finally {
                deleteBundle();
            }
            
            isDone = true;
            trace.info("Standalone application completed: return code=" + rc);
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
            if (!isDone())
                throw new TimeoutException();
            return rc;
        }
    }
}
