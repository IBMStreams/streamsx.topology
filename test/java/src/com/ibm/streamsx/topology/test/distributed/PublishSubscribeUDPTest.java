/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.distributed;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.test.distributed.PublishSubscribeTest.Delay;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

/**
 * Test publish/subscribe. These tests just use publish/subscribe
 * within a single job, but the expected use case is across jobs.
 *
 */
public class PublishSubscribeUDPTest extends TestTopology {

    @Before
    public void checkIsDistributed() {
        assumeTrue(getTesterType() == Type.DISTRIBUTED_TESTER);
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
        
        source = source.parallel(width);
        
        source = source.modify(new Delay<String>());
        
        source.publish(topic);
        
        TStream<String> subscribe = t.subscribe(topic, String.class);

        completeAndValidateUnordered(subscribe, 20, "325", "457", "9325", "hello", "udp");
    }
    
    @Test
    public void testPublishBothSubscribeNonUDP() throws Exception {
        
        String topic = "testPublishBothSubscribeNonUDP";
    
        final Topology t = new Topology();
        TStream<String> source = t.strings("325", "457", "9325", "hello", "udp");        
        source = source.parallel(4);      
        source = source.modify(new Delay<String>(10));     
        source.publish(topic);
        
        TStream<String> source2 = t.strings("non-udp", "single", "346");
        source2 = source2.modify(new Delay<String>(10));     
        source2.publish(topic);        
        
        
        TStream<String> subscribe = t.subscribe(topic, String.class);

        completeAndValidateUnordered(subscribe, 20, "325", "457", "9325", "hello", "udp", "non-udp", "single", "346");
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
        
        source = source.parallel(width);
        
        source = source.modify(new Delay<BigDecimal>());
        
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
        source = source.parallel(4);      
        source = source.modify(new Delay<BigDecimal>(10));     
        source.asType(BigDecimal.class).publish(topic);
        
        List<BigDecimal> data2 = new ArrayList<>(10);
        for (int i = 0; i < 10; i++)
            data2.add(new BigDecimal(r.nextDouble()* 100.0));

        
        TStream<BigDecimal> source2 = t.constants(data2);
        source2 = source2.modify(new Delay<BigDecimal>(10));     
        source2.asType(BigDecimal.class).publish(topic);   
        
        
        TStream<BigDecimal> subscribe = t.subscribe(topic, BigDecimal.class);
        TStream<String> strings = StringStreams.toString(subscribe);
        
        List<String> expected = new ArrayList<>();
        for (BigDecimal d : data)
            expected.add(d.toString());
        for (BigDecimal d : data2)
            expected.add(d.toString());

        completeAndValidateUnordered(strings, 20, expected.toArray(new String[0]));
    }
    
    public void completeAndValidateUnordered(
            TStream<String> output, int seconds, String...contents) throws Exception {
        
        Tester tester = output.topology().getTester();
        
        Condition<List<String>> expectedContents = tester.stringContentsUnordered(output, contents);
                
        tester.complete(
                getTesterContext(),
                getConfig(),
                expectedContents,
                seconds, TimeUnit.SECONDS);

        assertTrue(expectedContents.toString(), expectedContents.valid());
    }
    
}
