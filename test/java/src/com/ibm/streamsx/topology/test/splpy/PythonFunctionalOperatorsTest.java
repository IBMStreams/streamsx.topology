/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.splpy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.math.complex.Complex;
import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
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
  public static final StreamSchema ALL_PYTHON_TYPES_SCHEMA=
          Type.Factory.getStreamSchema("tuple<boolean b," +
    		  "int8 i8, int16 i16, int32 i32, int64 i64," +
    		  "uint8 u8, uint16 u16, uint32 u32, uint64 u64," +
    		  "float32 f32, float64 f64," +
    		  "rstring r," +
    		  "complex32 c32," +
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

    public static final StreamSchema ALL_PYTHON_TYPES_WITH_SETS_SCHEMA = ALL_PYTHON_TYPES_SCHEMA.extend("set<int32>", "si32"); 
    
    public static final int TUPLE_COUNT = 1000;
    
    @Before
    public void runSpl() {
        assumeSPLOk();
        
        assumeTrue(getTesterContext().getType() == StreamsContext.Type.STANDALONE_TESTER
        		|| getTesterContext().getType() == StreamsContext.Type.DISTRIBUTED_TESTER);
    }
    
    public static SPLStream testTupleStream(Topology topology) {
      return testTupleStream(topology, false);
    }

    public static StreamSchema getPythonTypesSchema(boolean withSets) {
      if (withSets) {
        return ALL_PYTHON_TYPES_WITH_SETS_SCHEMA;
      }
      else {
        return ALL_PYTHON_TYPES_SCHEMA;
      }
    }

    public static SPLStream testTupleStream(Topology topology, boolean withSets) {
        TStream<Long> beacon = BeaconStreams.longBeacon(topology, TUPLE_COUNT);

        return SPLStreams.convertStream(beacon, new BiFunction<Long, OutputTuple, OutputTuple>() {
            private static final long serialVersionUID = 1L;
            
            private transient TupleType type;
            private transient Random rand;

            @Override
            public OutputTuple apply(Long v1, OutputTuple v2) {
            	if (type == null) {
            		type = Type.Factory.getTupleType(getPythonTypesSchema(withSets).getLanguageType());
            		rand = new Random();
            	}
            	Tuple randTuple = (Tuple) type.randomValue(rand);
            	v2.assign(randTuple);
                return v2;
            }
        }, getPythonTypesSchema(withSets));
    }
    
    //@Test
    public void testPositionalSampleNoop() throws Exception {
        Topology topology = new Topology("testPositionalSampleNoop");
        
        SPLStream tuples = testTupleStream(topology);
        
        SPLStream viaSPL = SPL.invokeOperator("spl.relational::Functor", tuples, tuples.getSchema(), null);
        
        addTestToolkit(tuples);
        SPLStream viaPython = SPL.invokeOperator("com.ibm.streamsx.topology.pysamples.positional::Noop", tuples, tuples.getSchema(), null);

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(viaPython, TUPLE_COUNT);
        
        Condition<List<Tuple>> viaSPLResult = tester.tupleContents(viaSPL);
        Condition<List<Tuple>> viaPythonResult = tester.tupleContents(viaPython);
        
        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertEquals(viaSPLResult.getResult(), viaPythonResult.getResult());
    }
    
    static final StreamSchema TEST_SCHEMA_SF =
            Type.Factory.getStreamSchema("tuple<int32 a,  int16 b, int64 vl>");
    
    static final Tuple[] TEST_TUPLES = new Tuple[4];
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
                v2.assign(TEST_TUPLES[v1.intValue()]);
                return v2;
            }
        }, TEST_SCHEMA_SF);        
    }
    
    private static final AtomicBoolean extractedTestTookit = new AtomicBoolean();
    static void addTestToolkit(TopologyElement te) throws Exception {
        // Need to run extract to ensure the operators match the python
        // version we are testing.
    	
        File toolkitRoot = new File(getTestRoot(), "python/spl/testtkpy");
        
        // Only need to do this once.
		if (!extractedTestTookit.getAndSet(true)) {
			File lf = new File(toolkitRoot, ".lock");
			try (RandomAccessFile lff = new RandomAccessFile(lf, "rw");
					FileChannel channel = lff.getChannel();
					FileLock lock = channel.lock();
			) {
				int rc = PythonExtractTest.extract(toolkitRoot, true);
				assertEquals(0, rc);
			}
		}  
        
        SPL.addToolkit(te, toolkitRoot);
    }
    
    //@Test
    public void testPositionalSampleSimpleFilter() throws Exception {
        Topology topology = new Topology("testPositionalSampleSimpleFilter");
        
        SPLStream tuples = sampleFilterStream(topology);
        
        addTestToolkit(tuples);
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

        assertTrue(expectedCount.toString(), expectedCount.valid());
        assertTrue(viaPythonResult.toString(), viaPythonResult.valid());
    }
    
    //@Test
    public void testPositionalSampleSimpleFilterUsingSPLType() throws Exception {
        Topology topology = new Topology("testPositionalSampleSimpleFilterUsingSPLType");
        
        SPLStream tuples = sampleFilterStream(topology);
        
        addTestToolkit(tuples);
        SPLStream viaPython = SPL.invokeOperator(
                "testspl::SF", tuples, tuples.getSchema(), null);

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
    
    //@Test
    public void testStatefulOperator() throws Exception {
        Topology topology = new Topology("testPositionalSampleSimpleFilterUsingSPLType");
        
        SPLStream tuples = testTupleStream(topology, false);
        addTestToolkit(tuples);
        
        StreamSchema outSchema = tuples.getSchema().extend("int32", "sequence_using_py");
        SPLStream viaPython = SPL.invokeOperator(
                "com.ibm.streamsx.topology.pysamples.positional::AddSeq", tuples, outSchema, null);
        
        // Add a second count to make sure that the states are independent.
        SPLStream filtered = tuples.filter(t -> t.getInt("i32") < 10000);
        SPLStream viaPythonFiltered = SPL.invokeOperator(
                "com.ibm.streamsx.topology.pysamples.positional::AddSeq", filtered, outSchema, null);

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(viaPython, TUPLE_COUNT);
        Condition<List<Tuple>> outTuples = tester.tupleContents(viaPython);
        
        Condition<List<Tuple>> outFilteredTuples = tester.tupleContents(viaPythonFiltered);
        
        
        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        
        List<Tuple> result = outTuples.getResult();
        
        assertEquals(TUPLE_COUNT, result.size());
        for (int i = 0; i < TUPLE_COUNT; i++)
            assertEquals(i, result.get(i).getInt("sequence_using_py"));
        
        List<Tuple> filteredResult = outFilteredTuples.getResult();
        assertTrue(filteredResult.size() <= TUPLE_COUNT);
        
        for (int i = 0; i < filteredResult.size(); i++)
            assertEquals(i, filteredResult.get(i).getInt("sequence_using_py"));
    }
    
    //@Test
    public void testSourceWithClass() throws Exception {
        Topology topology = new Topology("testSourceWithClass");
        
        addTestToolkit(topology);
        
        StreamSchema outSchema = Type.Factory.getStreamSchema("tuple<int32 seq>");
        
        int count = new Random().nextInt(200) + 10;
        SPLStream pysrc = SPL.invokeSource(topology,
        		"com.ibm.streamsx.topology.pysamples.sources::Range",
        		Collections.singletonMap("count", count), outSchema);
        
        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(pysrc, count);
        Condition<List<Tuple>> outTuples = tester.tupleContents(pysrc);
        
        // getConfig().put(ContextProperties.TRACING_LEVEL, TraceLevel.DEBUG);
                
        complete(tester, expectedCount, 20, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        
        List<Tuple> result = outTuples.getResult();
        
        assertEquals(count, result.size());
        for (int i = 0; i < count; i++)
            assertEquals(i, result.get(i).getInt("seq"));
    }
    
    /**
     * Test that specific values in Python
     * make their way into SPL correctly.
     * @throws Exception
     */
    @Test
    public void testValues() throws Exception {
        Topology topology = new Topology("testValues");
        
        addTestToolkit(topology);
        
        SPLStream pysrc = SPL.invokeSource(topology,
        		"com.ibm.streamsx.topology.pytest.pysource::SpecificValues",
        		null, ALL_PYTHON_TYPES_SCHEMA);
        
        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(pysrc, 1);
        Condition<List<Tuple>> outTuples = tester.tupleContents(pysrc);
        
        // getConfig().put(ContextProperties.TRACING_LEVEL, TraceLevel.DEBUG);
                
        complete(tester, expectedCount, 20, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        
        Tuple r1 = outTuples.getResult().get(0);
        
        assertTrue(r1.getBoolean("b"));
        
        // signed integers
        // 23, -2525, 3252352, -2624565653,
        assertEquals(r1.getByte("i8"), 23);
        assertEquals(r1.getShort("i16"), -2525);
        assertEquals(r1.getInt("i32"), 3252352);
        assertEquals(r1.getLong("i64"), -2624565653L);
        
        // unsigned int
        // 72, 6873, 43665588, 357568872
        assertEquals(r1.getString("u8"), "72");
        assertEquals(r1.getString("u16"), "6873");
        assertEquals(r1.getString("u32"), "43665588");
        assertEquals(r1.getString("u64"), "357568872");
        
        // floats
        // 4367.34, -87657525334.22
        assertEquals(r1.getFloat("f32"), 4367.34f, 0.1);
        assertEquals(r1.getDouble("f64"), -87657525334.22d, 0.1);
        
        // rstring, Unicode data
        assertEquals("⡍⠔⠙⠖ ⡊ ⠙⠕⠝⠰⠞ ⠍⠑⠁⠝ ⠞⠕ ⠎⠁⠹ ⠹⠁⠞ ⡊ ⠅⠝⠪⠂ ⠕⠋ ⠍⠹", r1.getString("r"));
        
        // complex(-23.0, 325.38), complex(-35346.234, 952524.93)
        assertEquals(((Complex) r1.getObject("c32")).getReal(), -23.0, 0.1);
        assertEquals(((Complex) r1.getObject("c32")).getImaginary(), 325.38, 0.1);
        
        assertEquals(((Complex) r1.getObject("c64")).getReal(), -35346.234, 0.1);
        assertEquals(((Complex) r1.getObject("c64")).getImaginary(), 952524.93, 0.1);
        
        // ["a", "Streams!", "2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm"]
        {
        @SuppressWarnings("unchecked")
		List<RString> lr = (List<RString>) r1.getObject("lr");
        assertEquals(3, lr.size());
        assertEquals("a", lr.get(0).getString());
        assertEquals("Streams!", lr.get(1).getString());
        assertEquals("2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm", lr.get(2).getString());
        }
        
        //  [345,-4578],
        {
        int[] li32 = (int[]) r1.getObject("li32");
        assertEquals(2, li32.length);
        assertEquals(345, li32[0]);
        assertEquals(-4578, li32[1]);
        }

        // [9983, -4647787587, 0]
        {
        long[] li64 = (long[]) r1.getObject("li64");
        assertEquals(3, li64.length);
        assertEquals(9983L, li64[0]);
        assertEquals(-4647787587L, li64[1]);
        assertEquals(0L, li64[2]);
        }
        
        {
        @SuppressWarnings("unchecked")
		List<Integer> lui32 = (List<Integer>) r1.getObject("lui32");
        assertEquals(1, lui32.size());
        assertEquals("87346", Integer.toUnsignedString(lui32.get(0)));
        }
        
        {
        @SuppressWarnings("unchecked")
		List<Long> lui64 = (List<Long>) r1.getObject("lui64");
        assertEquals(2, lui64.size());
        assertEquals("45433674", Long.toUnsignedString(lui64.get(0)));
        assertEquals("41876984848", Long.toUnsignedString(lui64.get(1)));
        }
        
        // 4.269986E+05, -8.072285E+02 -6.917091E-08 7.735085E8
        {
            float[] li32 = (float[]) r1.getObject("lf32");
            assertEquals(4, li32.length);
            assertEquals(4.269986E+05f, li32[0], 0.1);
            assertEquals(-8.072285E+02f, li32[1], 0.1);
            assertEquals(-6.917091E-08f, li32[2], 0.1);
            assertEquals(7.735085E8f, li32[3], 0.1);
        }

    }
}
