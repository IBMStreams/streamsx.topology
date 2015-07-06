/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assume.assumeNotNull;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.JobProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.test.TestTopology;

/**
 * N.B. one or more of the tests require the STREAMSX_TOPOLOGY_DEFAULT_JOB_GROUP
 * environment variable to be set to a pre-existing non-default job group.
 * The test is skipped if that's not set.
 */
public class JobPropertiesTest extends TestTopology {

    @Test
    public void testNameProperty() throws Exception {
        testIt("testNameProperty", JobProperties.NAME,
                "JobPropertiesTestJobName");
    }

    @Test
    public void testGroupPropertyDefault() throws Exception {
        testIt("testGroupProperty", JobProperties.GROUP,
                // lame, but otherwise need a real pre-existing non-default one.
                "default");
    }

    @Test
    public void testGroupPropertyNonDefault() throws Exception {
        // NOT a real API EV... though perhaps should be.
        // Must name a pre-existing job group.
        // Skip the test if it's not set.
        String group = System.getenv("STREAMSX_TOPOLOGY_DEFAULT_JOB_GROUP");
        assumeNotNull(group);
        testIt("testGroupProperty", JobProperties.GROUP, group);
    }

    @Test
    public void testGroupPropertyNeg() throws Exception {
        testIt("testGroupProperty", JobProperties.GROUP,
                "myJobGroup-"+((long)(Math.random() + 10000)));
    }

    @Test
    public void testDataDirectoryProperty() throws Exception {
        testIt("testDataDirectoryProperty", JobProperties.DATA_DIRECTORY,
                "/no/such/path");
    }

    @Test
    public void testOverrideResourceLoadProtectionProperty() throws Exception {
        testIt("testOverrideResourceLoadProtectionProperty",
                JobProperties.OVERRIDE_RESOURCE_LOAD_PROTECTION, true);
    }

    @Test
    public void testPreloadApplicationBundlesProperty() throws Exception {
        testIt("testPreloadApplicationBundlesProperty",
                JobProperties.PRELOAD_APPLICATION_BUNDLES, true);
    }
    
    private void testIt(String topologyName, String propName, Object value)
            throws Exception {

        // JobProperties only apply to DISTRIBUTED submit
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);
        assumeTrue(SC_OK);
        
        // Full verification of the override or preload properties isn't practical
        // or really necessary for our API.  Streams doesn't provide any way to
        // query the job's state with respect to those settings.  We trust/require
        // that if we supply "submitjob" with the correct info it will do its job.
        //
        // Full verification testing for jobName and jobGroup is possible if the
        // test has a real instance to submit to.  Verification could do one of:
        // - capture / look at streamtool lsjob output
        // - use the domain mangement API.  This is complicated by having to
        //   supply login credentials and learning or being told the domain's
        //   mgmt server url.  Once the API does job submission via it, the
        //   pieces will be in place to make it easier to use it here too. 
        // It doesn't look like the SPL Java Operator API can provide an operator
        // with access to the analogous info nor a handle to the domain's mgmt api.
        //
        // So punt that for now and just verify that our API is supplying
        // the correct info to "submitjob".
        //
        // Given the current DistributedStreamsContext implementation (really
        // InvokeSubmit), we can capture the API's logging info to see the
        // actual submitjob cmd and then scan to verify the appropriate args
        // were supplied.
        //
        // We don't expect / require the actual submit to succeed with this
        // test strategy.  We don't need a live domain/instance. 

        getConfig().put(propName, value);
        ApiLog log = ApiLog.setup();
        log.getLogger().info("Hello topology=" + topologyName
                + " propName="+ propName
                + " value=" + value); 
        try {
            Topology topology = new Topology(topologyName);
            TStream<String> source = topology.strings("tuple1", "tuple2");

            // fyi using DISTRIBUTED_TESTER directly instead of
            // completeAndValidate() currently dies w/NPE.
            // We're using DISTRIBUTED_TESTER (and not just DISTRIBUTED)
            // because it's currently the only thing that can/does cancel
            // a submitted job.
            
            // The "negative" tests will cause a submitjob failure.
            // But we can still validate that we passed the right thing
            // to submitjob...
            
            try {
                completeAndValidate(source, 10,  "tuple1", "tuple2");
            }
            catch (Exception e) {
                log.getLogger().info("Got exception: " + e);
                // keep going as long as we have the real info we need
                if (getSubmitjobCmd(log)==null) {
                  e.printStackTrace();
                  fail(e.toString());
                }
            }
            
            assertTrue("propName="+getConfig().get(propName),
                    hasSubmitjobArg(propName, getConfig(), log));
        }
        finally {
            log.cleanup();
        }
    }
    
    /**
     * Check if the submitjob invocation included the proper info
     * for property configPropKey with value in config
     * @param configPropKey property to check
     * @param config map containing properties value
     * @param log API log
     * @return true iff submitjob invocation included the correct info
     */
    private boolean hasSubmitjobArg(String configPropKey,
            Map<String,Object> config, ApiLog log) {
        String submitjobCmd = getSubmitjobCmd(log);
        assertNotNull("submitjobCmd trace not located", submitjobCmd);
        
        PropChecker checker = streamtoolCheckers.get(configPropKey);
        return checker.isSet(config.get(configPropKey), submitjobCmd);
    }
    
    private String getSubmitjobCmd(ApiLog log) {
        // fyi sensitive to InvokeSubmit implementation
        String submitjobCmd = null;
        for(String s : log.getLines()) {
            if (s.indexOf("/bin/streamtool submitjob ") >= 0) {
                submitjobCmd = s;
                break;
            }
        }
        return submitjobCmd;
    }
    
    private interface PropChecker {
        boolean isSet(Object propValue, String cmd);
    }
    
    private static Map<String,PropChecker> streamtoolCheckers = 
            new HashMap<String,PropChecker>();
    static {
       streamtoolCheckers.put(JobProperties.NAME,
               new PropChecker() {
               public boolean isSet(Object propValue, String cmd) {
                   return cmd.indexOf(" --jobname "+propValue+" ") >= 0;
               }});
       streamtoolCheckers.put(JobProperties.GROUP,
               new PropChecker() {
               public boolean isSet(Object propValue, String cmd) {
                   return cmd.indexOf(" --jobgroup "+propValue+" ") >= 0;
               }});
       streamtoolCheckers.put(JobProperties.DATA_DIRECTORY,
               new PropChecker() {
               public boolean isSet(Object propValue, String cmd) {
                   return cmd.indexOf(" --config data-directory="+propValue+" ") >= 0;
               }});
       streamtoolCheckers.put(JobProperties.OVERRIDE_RESOURCE_LOAD_PROTECTION,
               new PropChecker() {
               public boolean isSet(Object propValue, String cmd) {
                   return cmd.indexOf(" --override HostLoadProtection ") >= 0;
               }});
       streamtoolCheckers.put(JobProperties.PRELOAD_APPLICATION_BUNDLES,
               new PropChecker() {
               public boolean isSet(Object propValue, String cmd) {
                   return cmd.indexOf(" --config preloadApplicationBundles="+propValue+" ") >= 0;
               }});
    }
    
    private static class ApiLog extends Handler {
        private List<String> lines = new ArrayList<String>(1000);
        private Level origLevel = null;
        
        private ApiLog(Level origLevel) {
            this.origLevel = origLevel;
        }
        
        public static ApiLog setup() {
            Logger logger = Topology.STREAMS_LOGGER;
            Level origLevel = logger.getLevel();
            if (origLevel==null || !logger.isLoggable(Level.INFO))
                logger.setLevel(Level.INFO);
            ApiLog log = new ApiLog(origLevel);
            logger.addHandler(log);
            return log;
        }
        
        public Logger getLogger() {
            return Topology.STREAMS_LOGGER;
        }
        
        public void cleanup() {
            Logger logger = Topology.STREAMS_LOGGER;
            logger.removeHandler(this);
            logger.setLevel(origLevel);
        }
        
        public List<String> getLines() {
            return lines;
        }

        @Override
        public void publish(LogRecord record) {
           lines.add(record.getMessage());
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() throws SecurityException {
            lines.clear();
        }
    }
}
