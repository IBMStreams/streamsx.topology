/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.samples;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ibm.streamsx.topology.test.TestTopology;

import parallel.ParallelRegexGrep;
import parallel.PartitionedParallelRegexGrep;
import simple.Grep;
import simple.RegexGrep;
import topic.PublishBeacon;
import topic.SubscribeBeacon;
import vwap.Vwap;

public class BundleSamplesTest extends TestTopology {
    
    /**
     * Just run as the main run, since none of these get executed.
     */
    @Before
    public void runAsMain() {
        assumeTrue(SC_OK);
        assumeTrue(isMainRun());
    }

    @Test
    public void testDistributedGrep() throws Exception {
        _testGrep("BUNDLE", "/tmp/books", "pattern");
    }

    @Test
    public void testStandaloneGrep() throws Exception {
        _testGrep("STANDALONE_BUNDLE", "/tmp/books", "pattern");
    }

    @Test
    public void testDistributedRegexGrep() throws Exception {
        _testRegexGrep("BUNDLE", "/tmp/books", ".*pattern.*");
    }

    @Test
    @Ignore
    public void testDistributedParallelRegexGrep() throws Exception {
        _testParallelRegexGrep("BUNDLE", "/tmp/books", ".*pattern.*");
    }

    @Test
    @Ignore
    public void testDistributedPartitionedParallelRegexGrep() throws Exception {
        _testPartitionedParallelRegexGrep("BUNDLE", "/tmp/books", ".*pattern.*");
    }

    @Test
    public void testStandaloneRegexGrep() throws Exception {
        _testRegexGrep("STANDALONE_BUNDLE", "/tmp/books", ".*pattern.*");
    }

    @Test
    public void testDistributedVwap() throws Exception {
        _testVwap("BUNDLE");
    }

    @Test
    public void testStandaloneVwap() throws Exception {
        _testVwap("STANDALONE_BUNDLE");
    }

    @Test
    public void testPublishBeacon() throws Exception {
        File bundle = new File("topic.PublishBeacon.sab");
        bundle.delete();
        assertFalse(bundle.exists());
        PublishBeacon.main(new String[] { "/atopic", "BUNDLE"});
        assertTrue(bundle.exists());
        bundle.delete();
        assertFalse(bundle.exists());
    }

    @Test
    public void testSubscribeBeacon() throws Exception {
        File bundle = new File("topic.SubscribeBeacon.sab");
        bundle.delete();
        assertFalse(bundle.exists());
        SubscribeBeacon.main(new String[] { "/atopic", "BUNDLE"});
        assertTrue(bundle.exists());
        bundle.delete();
        assertFalse(bundle.exists());
    }

    /**
     * Just test a bundle is created.
     */
    private void _testGrep(String... input) throws Exception {
        File bundle = new File("simple.Grep.sab");
        bundle.delete();
        assertFalse(bundle.exists());
        Grep.main(input);
        assertTrue(bundle.exists());
        bundle.delete();
        assertFalse(bundle.exists());
    }

    private void _testRegexGrep(String... input) throws Exception {
        File bundle = new File("simple.RegexGrep.sab");
        bundle.delete();
        assertFalse(bundle.exists());
        RegexGrep.main(input);
        assertTrue(bundle.exists());
        bundle.delete();
        assertFalse(bundle.exists());
    }

    private void _testParallelRegexGrep(String... input) throws Exception {
        File bundle = new File("parallel.ParallelRegexGrep.sab");
        bundle.delete();
        assertFalse(bundle.exists());
        ParallelRegexGrep.main(input);
        assertTrue(bundle.exists());
        bundle.delete();
        assertFalse(bundle.exists());
    }

    private void _testPartitionedParallelRegexGrep(String... input)
            throws Exception {
        File bundle = new File("parallel.PartitionedParallelRegexGrep.sab");
        bundle.delete();
        assertFalse(bundle.exists());
        PartitionedParallelRegexGrep.main(input);
        assertTrue(bundle.exists());
        bundle.delete();
        assertFalse(bundle.exists());
    }

    private void _testVwap(String... input) throws Exception {
        File bundle = new File("vwap.Vwap.sab");
        bundle.delete();
        assertFalse(bundle.exists());
        Vwap.main(input);
        assertTrue(bundle.exists());
        bundle.delete();
        assertFalse(bundle.exists());
    }

}
