/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.beam;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.test.api.StreamTest;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.context.StreamsContextFactory;

import com.ibm.streamsx.topology.streams.StringStreams;

public class ParDoTest extends TestTopology {

    private static class SimpleDoFn
            implements BiFunction<Long, 
                Map<Integer, List<?>>, 
                Map<Integer, List<?>>> {
        public Map<Integer, List<?>> apply(Long tuple, 
                Map<Integer, List<?>> sideInputs) {
            Map<Integer, List<?>> outputs = new HashMap<>();

            List<String> mainOutput = new LinkedList<String>();
            mainOutput.add(tuple.toString());
            for (Map.Entry<Integer, List<?>> entry: sideInputs.entrySet()) {
                for (Object obj: entry.getValue()) {
                    mainOutput.add(obj.toString());
                }
            }
            outputs.put(0, mainOutput);

            List<Long> sideOutput = new LinkedList<Long>();
            sideOutput.add(tuple);
            outputs.put(1, sideOutput);

            return outputs;
        }
    }

    @Test
    public void testParDo() throws Exception {
        final Topology topology = newTopology();

        // main input
        TStream<Long> cntSource = 
            topology.periodicSource(new CountingSupplier(), 1, TimeUnit.SECONDS);

        // side input 1
        TStream<Character> charSource1 = 
            topology.periodicSource(new CharacterSupplier(), 1, TimeUnit.SECONDS);
        // side input 2
        TStream<Character> charSource2 =
            topology.periodicSource(new CharacterSupplier(), 1, TimeUnit.SECONDS);

        TWindow<Character, Object> charWindow1 = charSource1.last(2);
        TWindow<Character, Object> charWindow2 = charSource2.last(4);

        List<TWindow<?, Object>> windows = new ArrayList<>(2);
        windows.add(charWindow1);
        windows.add(charWindow2);

        // output stream types
        List<Class<?>> outputTypes = new ArrayList<>(2);
        outputTypes.add(String.class);
        outputTypes.add(Long.class);

        List<TStream<?>> outputStreams = 
            cntSource.parDo(windows, new SimpleDoFn(), outputTypes);

        // main output is always at key 0
        TStream<String> mainOutput = (TStream<String>) outputStreams.get(0);
        // all following streams are side inputs
        TStream<Long> sideOutput = (TStream<Long>) outputStreams.get(1);

        Condition<Long> c = topology.getTester().atLeastTupleCount(sideOutput, 10);
        Condition<List<String>> tuples = topology.getTester().stringContents(mainOutput, "notused");

        complete(topology.getTester(), c, 15, TimeUnit.SECONDS);

        for (String tuple: tuples.getResult()) {
            System.out.print(tuple + ", ");
        }
    }
}
