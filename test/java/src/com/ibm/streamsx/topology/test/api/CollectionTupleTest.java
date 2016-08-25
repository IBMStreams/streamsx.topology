/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2016, 2016                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.streams.CollectionStreams;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;

@SuppressWarnings("serial")
public class CollectionTupleTest extends TestTopology {
	/* begin_generated_IBM_copyright_code                               */
	public static final String IBM_COPYRIGHT =
		" Licensed Materials-Property of IBM                              " + //$NON-NLS-1$ 
		" 5724-Y95                                                        " + //$NON-NLS-1$ 
		" (C) Copyright IBM Corp.  2016, 2016    All Rights Reserved.     " + //$NON-NLS-1$ 
		" US Government Users Restricted Rights - Use, duplication or     " + //$NON-NLS-1$ 
		" disclosure restricted by GSA ADP Schedule Contract with         " + //$NON-NLS-1$ 
		" IBM Corp.                                                       " + //$NON-NLS-1$ 
		"                                                                 " ; //$NON-NLS-1$ 
	/* end_generated_IBM_copyright_code                                 */

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
        final Topology topology = newTopology();
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
    
    @Test
    public void testSet() throws Exception {
        completeAndValidate(_testSet(), 10,  "a", "had", "lamb", "little", "mary", "as", "fleece", "its", "snow", "was", "white");
    }
    
    private static TStream<String> _testSet() throws Exception {
        TStream<List<String>> data = _listSource();
        TStream<Set<String>> set = data.transform(new Function<List<String>,Set<String>> () {

            @Override
            public Set<String> apply(List<String> v) {
                Set<String> set = new TreeSet<>();
                set.addAll(v);
                return set;
            }});
        
        return CollectionStreams.flatten(set); 
    }
    
    @Test
    public void testMap() throws Exception {
        completeAndValidate(_testMap(), 10,  "A=8", "B=32", "C=9", "D=73", "E=56");
    }
    
    private static TStream<String> _testMap() throws Exception {
        final Topology topology = newTopology();
        
        List<Map<String,Integer>> tuples = new ArrayList<>();
        
        Map<String,Integer> map = new TreeMap<>();
        
        map.put("A", 8);
        map.put("B", 32);
        tuples.add(map);
        
        map = new TreeMap<>();
        map.put("C", 9);
        map.put("D", 73);
        map.put("E", 56);
        tuples.add(map);
                
        TStream<Map<String,Integer>> data = topology.constants(tuples);
                            
        TStream<SimpleImmutableEntry<String, Integer>> entries = CollectionStreams.flattenMap(data);
        
        return StringStreams.toString(entries);
    }
    
    
}
