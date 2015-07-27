/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.streams.CollectionStreams;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;

@SuppressWarnings("serial")
public class CollectionTupleTest extends TestTopology {

    @Test
    public void testList() throws Exception {
        completeAndValidate(_testList(), 10,  "[mary, had, a, little, lamb]", "[its, fleece, was, white, as, snow]");
    }
    
    @Test
    public void testFlatten() throws Exception {
        
        TStream<String> words = CollectionStreams.flatten(_listSource());
        
        completeAndValidate(words, 10,  "mary", "had", "a", "little", "lamb", "its", "fleece", "was", "white", "as", "snow");
    }
    
    private static TStream<List<String>> _listSource() throws Exception {
        final Topology topology = new Topology();
        TStream<String> source = topology.strings("mary had a little lamb", "its fleece was white as snow");

        TStream<List<String>> words = source.transform(new Function<String,List<String>>() {

            @Override
            public List<String> apply(String v) {
                return Arrays.asList(v.split(" "));
            }});
        
        return words;
    }
    
    private static TStream<String> _testList() throws Exception {
        
        return StringStreams.toString(_listSource());     
    }
}
