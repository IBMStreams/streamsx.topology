/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.distributed;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLSchemas;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

/**
 * Test publish/subscribe. These tests just use publish/subscribe
 * within a single job, but the expected use case is across jobs.
 *
 */
public class PublishSubscribeUDPTest extends TestTopology {
    
    public PublishSubscribeUDPTest() {
        setStartupDelay(30);
    }

    @Before
    public void checkIsDistributed() {
        assumeTrue(isDistributedOrService());
    }
    
    @Test
    public void testPublishNonUDPSubscribeNonUDP() throws Exception {
        testPublishUDPSubscribeNonUDP(0);
    }

    @Test
    public void testPublishUDP1SubscribeNonUDP() throws Exception {
        testPublishUDPSubscribeNonUDP(1);
    }
    @Test
    public void testPublishUDP3SubscribeNonUDP() throws Exception {
        testPublishUDPSubscribeNonUDP(3);
    }
    
    private void testPublishUDPSubscribeNonUDP(int width) throws Exception {
        
        String topic = "testPublishUDPSubscribeNonUDP/" + width;
    
        final Topology t = new Topology();
        TStream<String> source = t.strings("325", "457", "9325", "hello", "udp");
        
        source = addStartupDelay(source);
        
        if (width > 0)
            source = source.parallel(width);
        
        source.publish(topic);
        
        TStream<String> subscribe = t.subscribe(topic, String.class);

        completeAndValidateUnordered(subscribe, 20, "325", "457", "9325", "hello", "udp");
    }
    
    @Test
    public void testPublishNonUDPUDP1() throws Exception {
        testPublishUDPSubscribeUDP(0, 1);
    }
    @Test
    public void testPublishNonUDPUDP3() throws Exception {
        testPublishUDPSubscribeUDP(0, 3);
    }
    @Test
    public void testPublishUDP1UDP3() throws Exception {
        testPublishUDPSubscribeUDP(1, 3);
    }
    @Test
    public void testPublishUDP4UDP3() throws Exception {
        testPublishUDPSubscribeUDP(4, 3);
    }
    
    private void testPublishUDPSubscribeUDP(int pwidth, int swidth) throws Exception {
        
        
        String topic = "testPublishUDPSubscribeUDP/" + pwidth;
        
        String[] data = new String[100];
        for (int i = 0; i < data.length; i++) {
            data[i] = "SubUDP" + i;
        }
    
        final Topology t = new Topology();
        
        SPL.addToolkit(t, new File(getTestRoot(), "spl/testtk"));
               
        TStream<String> source = t.strings(data);
        
        if (pwidth > 0)
            source = source.parallel(pwidth);
        
        source = addStartupDelay(source);
        
        source.publish(topic);
        
        Map<String,Object> params = new HashMap<>();
        params.put("width", swidth);
        params.put("topic", topic);
        
        SPLStream subscribe = SPL.invokeSource(t, "testspl::UDPStringSub", params, SPLSchemas.STRING);
        
        completeAndValidateUnordered(subscribe.toStringStream(), 30, data);
    }
    
    @Test
    public void testPublishBothSubscribeNonUDP() throws Exception {
        
        String topic = "testPublishBothSubscribeNonUDP";
    
        final Topology t = new Topology();
        TStream<String> source = t.strings("325", "457", "9325", "hello", "udp");   
        setStartupDelay(20);
        source = addStartupDelay(source);   

        source = source.parallel(4);     
        source.publish(topic);
        
        TStream<String> source2 = t.strings("non-udp", "single", "346");
        source2 = addStartupDelay(source2);     
        source2.publish(topic);        
               
        TStream<String> subscribe = t.subscribe(topic, String.class);

        completeAndValidateUnordered(subscribe, 40, "325", "457", "9325", "hello", "udp", "non-udp", "single", "346");
    }
    
    @Test
    public void testObjectPublishNonUDPSubscribeNonUDP() throws Exception {
        testObjectPublishUDPSubscribeNonUDP(0);
    }
    
    @Test
    public void testObjectPublishUDP1SubscribeNonUDP() throws Exception {
        testObjectPublishUDPSubscribeNonUDP(1);
    }
    @Test
    public void testObjectPublishUDP3SubscribeNonUDP() throws Exception {
        testObjectPublishUDPSubscribeNonUDP(3);
    }
    
    private void testObjectPublishUDPSubscribeNonUDP(int width) throws Exception {
        
        String topic = "testObjectPublishUDPSubscribeNonUDP/" + width;
        
        Random r = new Random();
        List<BigDecimal> data = new ArrayList<>(20);
        for (int i = 0; i < 20; i++)
            data.add(new BigDecimal(r.nextDouble()* 100.0));
    
        final Topology t = new Topology();
        TStream<BigDecimal> source = t.constants(data);
              
        source = addStartupDelay(source);
        
        if (width > 0)
            source = source.parallel(width);
    
        source.asType(BigDecimal.class).publish(topic);
        
        TStream<BigDecimal> subscribe = t.subscribe(topic, BigDecimal.class);
        TStream<String> strings = StringStreams.toString(subscribe);
        
        List<String> expected = new ArrayList<>();
        for (BigDecimal d : data)
            expected.add(d.toString());

        completeAndValidateUnordered(strings, 20, expected.toArray(new String[0]));
    }
    
    @Test
    public void testObjectPublishBothSubscribeNonUDP() throws Exception {
        
        String topic = "testObjectPublishBothSubscribeNonUDP";
        
        Random r = new Random();
        List<BigDecimal> data = new ArrayList<>(20);
        for (int i = 0; i < 20; i++)
            data.add(new BigDecimal(r.nextDouble()* 100.0));
 
        final Topology t = new Topology();
        TStream<BigDecimal> source = t.constants(data);        
        source = addStartupDelay(source);     
        source = source.parallel(4);  
        source.asType(BigDecimal.class).publish(topic);
        
        List<BigDecimal> data2 = new ArrayList<>(10);
        for (int i = 0; i < 10; i++)
            data2.add(new BigDecimal(r.nextDouble()* 100.0));

        
        TStream<BigDecimal> source2 = t.constants(data2);
        source2 = addStartupDelay(source2);     
        source2.asType(BigDecimal.class).publish(topic);   
        
        
        TStream<BigDecimal> subscribe = t.subscribe(topic, BigDecimal.class);
        TStream<String> strings = StringStreams.toString(subscribe);
        
        List<String> expected = new ArrayList<>();
        for (BigDecimal d : data)
            expected.add(d.toString());
        for (BigDecimal d : data2)
            expected.add(d.toString());

        completeAndValidateUnordered(strings, 40, expected.toArray(new String[0]));
    }
    
    public void completeAndValidateUnordered(
            TStream<String> output, int seconds, String...contents) throws Exception {
        
        Tester tester = output.topology().getTester();
        
        Condition<List<String>> expectedContents = tester.stringContentsUnordered(output, contents);
                
        tester.complete(
                getTesterContext(),
                getConfig(),
                expectedContents,
                seconds + getStartupDelay(), TimeUnit.SECONDS);

        assertTrue(expectedContents.toString(), expectedContents.valid());
    }
 
}
