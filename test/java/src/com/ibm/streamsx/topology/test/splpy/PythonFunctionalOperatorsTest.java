/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.splpy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.streams.BeaconStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class PythonFunctionalOperatorsTest extends TestTopology {
    
    public static final StreamSchema ALL_PYTHON_TYPES_SCHEMA =
            Type.Factory.getStreamSchema("tuple<boolean b," +
    		  "int8 i8, int16 i16, int32 i32, int64 i64," +
    		  "uint8 u8, uint16 u16, uint32 u32, uint64 u64," +
    		  "float32 f32, float64 f64," +
    		  "rstring r," +
    		  "complex64 c64," +
    		  "list<rstring> lr," +
    		  "list<int32> li32," +
    		  "list<int64> li64," +
    		  "list<uint32> lui32," +
    		  "list<uint64> lui64," +
    		  "list<float32> lf32," +
    		  "list<float64> lf64," +
    		  "list<boolean> lb," +
    		  "map<int32,rstring> mi32r," +
    		  "map<rstring,uint32> mru32," +
    		  "map<rstring,int32> mri32," +
    		  "map<uint32,rstring> mu32r," +
    		  "map<int32,int32> mi32i32," +
    		  "map<uint32,uint32> mu32u32," +
    		  "map<rstring,rstring> mrr," +
    		  "map<float64,float64> mf64f64," +
    		  "map<float64,int32> mf64i32," +
    		  "map<float64,uint32> mf64u32," +
    		  "map<float64,rstring> mf64r," +
    		  "map<rstring,float64> mrf64>");

    
    private static final int TUPLE_COUNT = 1000;
    
    @Before
    public void runSpl() {
        assumeSPLOk();
        
        assumeTrue(getTesterContext().getType() == StreamsContext.Type.STANDALONE_TESTER
        		|| getTesterContext().getType() == StreamsContext.Type.DISTRIBUTED_TESTER);
    }
    
    public static SPLStream testTupleStream(Topology topology) {
        TStream<Long> beacon = BeaconStreams.longBeacon(topology, TUPLE_COUNT);

        return SPLStreams.convertStream(beacon, new BiFunction<Long, OutputTuple, OutputTuple>() {
            private static final long serialVersionUID = 1L;
            
            private transient TupleType type;
            private transient Random rand;

            @Override
            public OutputTuple apply(Long v1, OutputTuple v2) {
            	if (type == null) {
            		type = Type.Factory.getTupleType(ALL_PYTHON_TYPES_SCHEMA.getLanguageType());
            		rand = new Random();
            	}
            	Tuple randTuple = (Tuple) type.randomValue(rand);
            	v2.assign(randTuple);
                return v2;
            }
        }, ALL_PYTHON_TYPES_SCHEMA);        
    }
    
    @Test
    public void testPositionalSampleNoop() throws Exception {
        Topology topology = new Topology("testPositionalSampleNoop");
        
        SPLStream tuples = testTupleStream(topology);
        
        SPLStream viaSPL = SPL.invokeOperator("spl.relational::Functor", tuples, tuples.getSchema(), null);
        
        SPL.addToolkit(tuples, new File(getTestRoot(), "python/spl/testtkpy"));
        SPLStream viaPython = SPL.invokeOperator("com.ibm.streamsx.topology.pysamples.positional::Noop", tuples, tuples.getSchema(), null);

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(viaPython, TUPLE_COUNT);
        
        Condition<List<Tuple>> viaSPLResult = tester.tupleContents(viaSPL);
        Condition<List<Tuple>> viaPythonResult = tester.tupleContents(viaPython);

        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertEquals(viaSPLResult.getResult(), viaPythonResult.getResult());
    }
    
    private static final StreamSchema TEST_SCHEMA_SF =
            Type.Factory.getStreamSchema("tuple<int32 a,  int16 b, int64 vl>");
    
    private static final Tuple[] TEST_TUPLES = new Tuple[4];
    static {
        TEST_TUPLES[0] = TEST_SCHEMA_SF.getTuple(new Object[] {7, (short) 25, 34535L});
        TEST_TUPLES[1] = TEST_SCHEMA_SF.getTuple(new Object[] {32, (short) 6, 43675232L});
        TEST_TUPLES[2] = TEST_SCHEMA_SF.getTuple(new Object[] {2, (short) 3, 654932L});
        TEST_TUPLES[3] = TEST_SCHEMA_SF.getTuple(new Object[] {431221, (short) 1321, 82343L});
    }
    
    public static SPLStream sampleFilterStream(Topology topology) {
        TStream<Long> beacon = BeaconStreams.longBeacon(topology, TEST_TUPLES.length);

        return SPLStreams.convertStream(beacon, new BiFunction<Long, OutputTuple, OutputTuple>() {
            private static final long serialVersionUID = 1L;

            @Override
            public OutputTuple apply(Long v1, OutputTuple v2) {
                v2.assign(TEST_TUPLES[(int)((long) v1)]);
                return v2;
            }
        }, TEST_SCHEMA_SF);        
    }
    
    @Test
    public void testPositionalSampleSimpleFilter() throws Exception {
        Topology topology = new Topology("testPositionalSampleSimpleFilter");
        
        SPLStream tuples = sampleFilterStream(topology);
        
        SPL.addToolkit(tuples, new File(getTestRoot(), "python/spl/testtkpy"));
        SPLStream viaPython = SPL.invokeOperator(
        		"com.ibm.streamsx.topology.pysamples.positional::SimpleFilter", tuples, tuples.getSchema(), null);

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(viaPython, 2);
        
        // first attribute is the sum of the first and second input attributes
        // others are copied across from in to out.
        Tuple r1 = TEST_SCHEMA_SF.getTuple(new Object[] {32, (short) 25, 34535L});
        Tuple r2 = TEST_SCHEMA_SF.getTuple(new Object[] {5, (short) 3, 654932L});
        Condition<List<Tuple>> viaPythonResult = tester.tupleContents(viaPython,
        		r1, r2);

        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(viaPythonResult.toString(), viaPythonResult.valid());
    }

}
