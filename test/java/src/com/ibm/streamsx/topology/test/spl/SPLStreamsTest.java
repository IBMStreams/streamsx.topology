/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.spl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function7.BiFunction;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.streams.BeaconStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class SPLStreamsTest extends TestTopology {
    
    public static final StreamSchema TEST_SCHEMA =
            Type.Factory.getStreamSchema("tuple<int32 id, int32 vi, int64 vl>");
    
    public static final Tuple[] TEST_TUPLES = new Tuple[4];
    static {
        TEST_TUPLES[0] = TEST_SCHEMA.getTuple(new Object[] {0, 321, 34535L});
        TEST_TUPLES[1] = TEST_SCHEMA.getTuple(new Object[] {1, 235, 43675232L});
        TEST_TUPLES[2] = TEST_SCHEMA.getTuple(new Object[] {2, 321, 654932L});
        TEST_TUPLES[3] = TEST_SCHEMA.getTuple(new Object[] {3, 32525, 82343L});
    }
    
    public static SPLStream testTupleStream(Topology topology) {
        TStream<Long> beacon = BeaconStreams.longBeacon(topology, TEST_TUPLES.length);

        return SPLStreams.convertStream(beacon, new BiFunction<Long, OutputTuple, OutputTuple>() {
            private static final long serialVersionUID = 1L;

            @Override
            public OutputTuple apply(Long v1, OutputTuple v2) {
                v2.assign(TEST_TUPLES[(int)((long) v1)]);
                return v2;
            }
        }, TEST_SCHEMA);        
    }
    
    @Test
    public void testTuples() throws Exception {
        Topology topology = new Topology("testTuples");
        
        SPLStream tuples = testTupleStream(topology);

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(tuples, TEST_TUPLES.length);
        Condition<List<Tuple>> expectedTuples = tester.tupleContents(tuples, TEST_TUPLES);

        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(expectedTuples.toString(), expectedTuples.valid());
    }


    public static class IntAndString implements Serializable {
        public int n;
        public String s;

        @Override
        public String toString() {
            return "n:" + n + " s:" + s;
        }
    }

    @Test
    public void testConversionToSPL() throws Exception {
        final Topology topology = new Topology("ConvertSPLStream");
        SPLStream splStream = createSPLFlowFromStream(topology, false);
        TStream<String> tupleString = splStream.toTupleString();
        
        completeAndValidate(tupleString, 10,  "{ii=418,ss=\"325\"}",
                "{ii=550,ss=\"457\"}", "{ii=9418,ss=\"9325\"}");
    }

    @Test
    public void testConversionToSPLWithSkip() throws Exception {
        final Topology topology = new Topology("ConvertSPLStream");
        SPLStream splStream = createSPLFlowFromStream(topology, true);
        TStream<String> tupleString = splStream.toTupleString();

        completeAndValidate(tupleString, 10, "{ii=418,ss=\"325\"}",
                "{ii=9418,ss=\"9325\"}");
    }

    @Test
    public void testConversionFromSPL() throws Exception {
        final Topology topology = new Topology("ConvertSPLStream");
        SPLStream splStream = createSPLFlowFromStream(topology, false);
        TStream<IntAndString> iands = createStreamFromSPLStream(splStream);
        assertEquals(IntAndString.class, iands.getTupleClass());

        completeAndValidate(iands, 10,  "n:465 s:325", "n:597 s:457",
                "n:9465 s:9325");
    }
    
    @Test
    public void testConversionFromSPLToString() throws Exception {
        final Topology topology = new Topology();
        SPLStream splStream = testTupleStream(topology);
        TStream<String> strings = SPLStreams.toStringStream(splStream);
        assertEquals(String.class, strings.getTupleClass());

        completeAndValidate(strings, 10,  "0", "1", "2", "3");
    }
    @Test
    public void testConversionFromSPLToStringAttribute() throws Exception {
        final Topology topology = new Topology();
        SPLStream splStream = testTupleStream(topology);
        TStream<String> strings = SPLStreams.toStringStream(splStream, "vl");
        assertEquals(String.class, strings.getTupleClass());

        completeAndValidate(strings, 10,  "34535", "43675232", "654932", "82343");
    }

    private static TStream<IntAndString> createStreamFromSPLStream(
            SPLStream stream) {
        return stream.convert(new Function<Tuple, IntAndString>() {

            @Override
            public IntAndString apply(Tuple v1) {
                IntAndString ias = new IntAndString();
                ias.n = v1.getInt("ii") + 47;
                ias.s = v1.getString("ss");
                return ias;
            }
        }, IntAndString.class);
    }

    private static SPLStream createSPLFlowFromStream(final Topology topology,
            final boolean skipSecond) {
        TStream<String> source = topology.strings("325", "457", "9325");

        TStream<IntAndString> iands = source.transform(
                new Function<String, IntAndString>() {

                    @Override
                    public IntAndString apply(String v1) {
                        IntAndString is = new IntAndString();
                        is.s = v1;
                        is.n = Integer.valueOf(v1) + 93;
                        return is;
                    }
                }, IntAndString.class);

        StreamSchema schema = Type.Factory
                .getStreamSchema("tuple<int32 ii, rstring ss>");
        SPLStream splStream = SPLStreams.convertStream(iands,
                new BiFunction<IntAndString, OutputTuple, OutputTuple>() {

                    @Override
                    public OutputTuple apply(IntAndString v1, OutputTuple v2) {
                        if (skipSecond & v1.n == 550)
                            return null;

                        v2.setString("ss", v1.s);
                        v2.setInt("ii", v1.n);
                        return v2;
                    }
                }, schema);

        assertEquals(schema, splStream.getSchema());
        return splStream;
    }
}
