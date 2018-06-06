/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.splpy;

import static com.ibm.streams.operator.version.Product.getVersion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.streams.BeaconStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class PythonFunctionalOperatorsTest extends TestTopology {
    
  // Need to match schema in test/python/pubsub/pytest_schema.py
  public static final StreamSchema ALL_PYTHON_TYPES_SCHEMA=
          Type.Factory.getStreamSchema("tuple<boolean b," +
    		  "int8 i8, int16 i16, int32 i32, int64 i64," +
    		  "uint8 u8, uint16 u16, uint32 u32, uint64 u64," +
    		  "float32 f32, float64 f64," +
    		  "rstring r," +
    		  "complex32 c32," +
    		  "complex64 c64," +
                  "decimal32 d32," +
                  "decimal64 d64," +
                  "decimal128 d128," +
    		  "timestamp ts," +
                  "blob binary," +
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
    		  "map<rstring,float64> mrf64," +
    		  "list<list<float64>> llf64," +
    		  "map<rstring,list<int32>> mrli32," +
    		  "map<rstring,map<rstring,float64>> mrmrf64," +
    		  "set<int32> si32" +
    		  ">");

    public static final StreamSchema ALL_PYTHON_TYPES_WITH_SETS_SCHEMA = ALL_PYTHON_TYPES_SCHEMA; 

    public static final String PYTHON_OPTIONAL_TYPES_SCHEMA_STRING =
          "tuple<" +
    		  "optional<int32> oi32v, optional<int32> oi32nv," +
    		  "optional<list<rstring>> olrv, optional<list<rstring>> olrnv" +
    		  ">";
    
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

        SPLStream tuples = SPLStreams.convertStream(beacon, new BiFunction<Long, OutputTuple, OutputTuple>() {
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

        return tuples;
    }
    
    @Test
    public void testPositionalSampleNoop() throws Exception {
        Topology topology = new Topology("testPositionalSampleNoop");
        addTestToolkit(topology);
        
        SPLStream tuples = testTupleStream(topology);
        

        
        SPLStream viaSPL = SPL.invokeOperator("spl.relational::Functor", tuples, tuples.getSchema(), null);      

        SPLStream viaPython = SPL.invokeOperator("com.ibm.streamsx.topology.pysamples.positional::Noop", tuples, tuples.getSchema(), null);
        
        // Test accessing the execution context provides the correct results
        // Only supported for Python 3.5 and Streams 4.2 and later
        if ((getVersion().getVersion() > 4) ||
                (getVersion().getVersion() == 4 && getVersion().getRelease() >= 2)) {
            viaPython = SPL.invokeOperator(
                    "com.ibm.streamsx.topology.pytest.pyec::TestOperatorContext", viaPython,
                    viaPython.getSchema(), null);

            viaPython = SPL.invokeOperator(
                    "com.ibm.streamsx.topology.pytest.pyec::PyTestMetrics", viaPython,
                    viaPython.getSchema(), null);
        }

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(viaPython, TUPLE_COUNT);
        
        Condition<List<Tuple>> viaSPLResult = tester.tupleContents(viaSPL);
        Condition<List<Tuple>> viaPythonResult = tester.tupleContents(viaPython);
        
        complete(tester, expectedCount, 60, TimeUnit.SECONDS);

        assertTrue(expectedCount.getResult().toString(), expectedCount.valid());
        List<Tuple> viaspl = viaSPLResult.getResult();
        List<Tuple> viapython = viaPythonResult.getResult();
        for (int i = 0; i < viaspl.size(); i++)
            assertEquals(viaspl.get(i), viapython.get(i));
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
    
    static void addTestToolkit(TopologyElement te) throws Exception {    	
        File toolkitRoot = new File(getTestRoot(), "python/spl/testtkpy");
        
        SPL.addToolkit(te, toolkitRoot);
    }
    
    @Test
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
    
    @Test
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
    
    @Test
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
        
        
        complete(tester, expectedCount, 60, TimeUnit.SECONDS);

        assertTrue(expectedCount.getResult().toString(), expectedCount.valid());
        
        List<Tuple> result = outTuples.getResult();
        
        assertEquals(TUPLE_COUNT, result.size());
        for (int i = 0; i < TUPLE_COUNT; i++)
            assertEquals(i, result.get(i).getInt("sequence_using_py"));
        
        List<Tuple> filteredResult = outFilteredTuples.getResult();
        assertTrue(filteredResult.size() <= TUPLE_COUNT);
        
        for (int i = 0; i < filteredResult.size(); i++)
            assertEquals(i, filteredResult.get(i).getInt("sequence_using_py"));
    }
    
    @Test
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
     * make their way into SPL correctly
     * when returning as a tuple.
     * @throws Exception
     */
    @Test
    public void testValues() throws Exception {
        Topology topology = new Topology("testValues");
        
        addTestToolkit(topology);
        
        SPLStream pysrc = SPL.invokeSource(topology,
        		"com.ibm.streamsx.topology.pytest.pysource::SpecificValues",
        		null, ALL_PYTHON_TYPES_SCHEMA);
        
        StreamSchema sparseSchema = Type.Factory.getStreamSchema("tuple<int32 a, int32 b, int32 c, int32 d, int32 e>");
        
              
        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(pysrc, 1);
        Condition<List<Tuple>> outTuples = tester.tupleContents(pysrc);
        
        SPLStream pysparse = SPL.invokeSource(topology,
        		"com.ibm.streamsx.topology.pytest.pysource::SparseTuple",
        		null, sparseSchema);
        SPLStream pysparsemap = SPL.invokeOperator("com.ibm.streamsx.topology.pytest.pymap::SparseTupleMap",
        		pysparse, sparseSchema.extend("int32", "f"), null);

        Condition<Long> expectedCountSparse = tester.tupleCount(pysparse, 1);
        Condition<List<Tuple>> sparseTupleOut = tester.tupleContents(pysparse);
        
        Condition<Long> expectedCountSparseMap = tester.tupleCount(pysparsemap, 1);
        Condition<List<Tuple>> sparseTupleMapOut = tester.tupleContents(pysparsemap);
        
        // getConfig().put(ContextProperties.TRACING_LEVEL, TraceLevel.DEBUG);
                
        complete(tester, expectedCount.and(expectedCountSparse, expectedCountSparseMap), 20, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(expectedCountSparse.valid());
        assertTrue(expectedCountSparseMap.valid());
        
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
        
        assertEquals(r1.getBigDecimal("d32"), new BigDecimal("3.459876E72"));
        assertEquals(r1.getBigDecimal("d64"), new BigDecimal("4.515716038731674E-307"));
        assertEquals(r1.getBigDecimal("d128"), new BigDecimal("1.085059319410602846995696978141388E+5922"));

        // Timestamp Timestamp(781959759, 9320, 76)
        assertEquals(781959759L, r1.getTimestamp("ts").getSeconds());
        assertEquals(9320, r1.getTimestamp("ts").getNanoseconds());
        assertEquals(76, r1.getTimestamp("ts").getMachineId());
        
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
        
        {
            double[] lf64 = (double[]) r1.getObject("lf64");
            assertEquals(1, lf64.length);
            assertEquals(765.46477e19, lf64[0], 0.1);
        }
        
        {
            boolean[] lb = (boolean[]) r1.getObject("lb");
            assertEquals(3, lb.length);
            assertTrue(lb[0]);
            assertFalse(lb[1]);
            assertTrue(lb[2]);
        }
        
        assertTrue(r1.getMap("mi32r").isEmpty());
        assertTrue(r1.getMap("mru32").isEmpty());
        
        {
            Map<?,?> mri32  = r1.getMap("mri32");
            assertEquals(2, mri32.size());
            System.out.println("mri32:"  + mri32);
            assertTrue(mri32.containsKey(new RString("abc")));
            assertTrue(mri32.containsKey(new RString("многоязычных")));
            
            assertEquals(35320, mri32.get(new RString("abc")));
            assertEquals(-236325, mri32.get(new RString("многоязычных")));
        }
        
        assertTrue(r1.getMap("mu32r").isEmpty());
        assertTrue(r1.getMap("mi32i32").isEmpty());
        assertTrue(r1.getMap("mu32u32").isEmpty());
        assertTrue(r1.getMap("mrr").isEmpty());
        assertTrue(r1.getMap("mf64f64").isEmpty());
        assertTrue(r1.getMap("mf64i32").isEmpty());
        assertTrue(r1.getMap("mf64u32").isEmpty());
        assertTrue(r1.getMap("mf64r").isEmpty());
        assertTrue(r1.getMap("mrf64").isEmpty());
        
        // Sparse tuple handling - source
        assertEquals(1, sparseTupleOut.getResult().size());
        Tuple st = sparseTupleOut.getResult().get(0);
        assertEquals(37, st.getInt("a")); // set by op
        assertEquals(0, st.getInt("b")); // default as None in tuple
        assertEquals(0, st.getInt("c")); // default as None in tuple
        assertEquals(-46, st.getInt("d")); // set by op
        assertEquals(0, st.getInt("e")); // default as no value (short tuple)
        
        // Sparse tuple handling - map
        assertEquals(1, sparseTupleMapOut.getResult().size());
        Tuple stm = sparseTupleMapOut.getResult().get(0);
        assertEquals(37+81, stm.getInt("a")); // set by op
        assertEquals(23, stm.getInt("b")); // set by op
        assertEquals(0, stm.getInt("c")); // default as None in tuple
        assertEquals(-46, stm.getInt("d")); // default to matching input
        assertEquals(34, stm.getInt("e")); // set by op
        assertEquals(0, stm.getInt("f")); // default as no value (short tuple)
    }

    /**
     * Test that specific values in Python
     * make their way into SPL correctly
     * when returning as a dictionary.
     * @throws Exception
     */
    @Test
    public void testReturnDict() throws Exception {
        Topology topology = new Topology("testReturnDict");
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        
        addTestToolkit(topology);
               
        StreamSchema schema = Type.Factory.getStreamSchema("tuple<int32 a, int32 b, int32 c, int32 d, int32 e>");
        
        SPLStream pyds = SPL.invokeSource(topology,
        		"com.ibm.streamsx.topology.pytest.pysource::DictTuple",
        		null, schema);
        
        SPLStream pydm = SPL.invokeOperator("com.ibm.streamsx.topology.pytest.pymap::DictTupleMap",
        		pyds, schema, null);
            
        Tester tester = topology.getTester();
        Condition<?> expectedCount = tester.tupleCount(pyds, 4).and(tester.tupleCount(pydm, 4));
        Condition<List<Tuple>> outTuples = tester.tupleContents(pyds);
        Condition<List<Tuple>> outTuplesMap = tester.tupleContents(pydm);
                      
        complete(tester, expectedCount, 20, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());  
        
        // Dict tuple handling - source
        Tuple r1 = outTuples.getResult().get(0);
        assertEquals(3245, r1.getInt("a"));
        assertEquals(0, r1.getInt("b"));
        assertEquals(93, r1.getInt("c"));
        assertEquals(0, r1.getInt("d"));
        assertEquals(0, r1.getInt("e"));
        
        Tuple r2 = outTuples.getResult().get(1);
        assertEquals(831, r2.getInt("a"));
        assertEquals(421, r2.getInt("b"));
        assertEquals(0, r2.getInt("c"));
        assertEquals(-4455, r2.getInt("d"));
        assertEquals(0, r2.getInt("e"));
        
        Tuple r3 = outTuples.getResult().get(2);
        assertEquals(1, r3.getInt("a"));
        assertEquals(2, r3.getInt("b"));
        assertEquals(3, r3.getInt("c"));
        assertEquals(4, r3.getInt("d"));
        assertEquals(5, r3.getInt("e"));
        
        Tuple r4 = outTuples.getResult().get(3);
        assertEquals(0, r4.getInt("a"));
        assertEquals(-32, r4.getInt("b"));
        assertEquals(0, r4.getInt("c"));
        assertEquals(0, r4.getInt("d"));
        assertEquals(-64, r4.getInt("e"));
        
        // Now the map
        Tuple m1 = outTuplesMap.getResult().get(0);
        assertEquals(3245, m1.getInt("a"));
        assertEquals(120, m1.getInt("b"));
        assertEquals(93, m1.getInt("c"));
        assertEquals(0, m1.getInt("d"));
        assertEquals(0, m1.getInt("e"));
        
        Tuple m2 = outTuplesMap.getResult().get(1);
        assertEquals(1, m2.getInt("a"));
        assertEquals(2, m2.getInt("b"));
        assertEquals(3, m2.getInt("c"));
        assertEquals(4, m2.getInt("d"));
        assertEquals(5, m2.getInt("e"));
        
        Tuple m3 = outTuplesMap.getResult().get(2);
        assertEquals(1, m3.getInt("a"));
        assertEquals(2, m3.getInt("b"));
        assertEquals(23, m3.getInt("c"));
        assertEquals(24, m3.getInt("d"));
        assertEquals(25, m3.getInt("e"));
        
        Tuple m4 = outTuplesMap.getResult().get(3);
        assertEquals(0, m4.getInt("a"));
        assertEquals(-39, m4.getInt("b"));
        assertEquals(0, m4.getInt("c"));
        assertEquals(0, m4.getInt("d"));
        assertEquals(-64, m4.getInt("e"));
    }
    
    /**
     * Test that specific values in Python for optional types
     * make their way into SPL correctly
     * when returning as a tuple.
     * @throws Exception
     */
    @Test
    public void testValuesForOptionalTypes() throws Exception {
        assumeOptionalTypes();
        Topology topology = new Topology("testValuesForOptionalTypes");
        
        addTestToolkit(topology);
        
        SPLStream pysrc = SPL.invokeSource(topology,
        		"com.ibm.streamsx.topology.pytest.pysource.opttype::SpecificValues",
        		null, Type.Factory.getStreamSchema(PYTHON_OPTIONAL_TYPES_SCHEMA_STRING));
        
        StreamSchema sparseSchema = Type.Factory.getStreamSchema("tuple<optional<int32> a, optional<int32> b, optional<int32> c, optional<int32> d, optional<int32> e, optional<int32> f, int32 g, int32 h, optional<int32> i>");

        StreamSchema sparseSchemaMap = Type.Factory.getStreamSchema("tuple<optional<int32> a, optional<int32> b, optional<int32> c, optional<int32> d, optional<int32> e, int32 f, int32 g, optional<int32> h, optional<int32> i>");
              
        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(pysrc, 1);
        Condition<List<Tuple>> outTuples = tester.tupleContents(pysrc);
        
        SPLStream pysparse = SPL.invokeSource(topology,
        		"com.ibm.streamsx.topology.pytest.pysource.opttype::SparseTuple",
        		null, sparseSchema);
        SPLStream pysparsemap = SPL.invokeOperator("com.ibm.streamsx.topology.pytest.pymap.opttype::SparseTupleMap",
        		pysparse, sparseSchemaMap.extend("optional<int32>", "j"), null);

        Condition<Long> expectedCountSparse = tester.tupleCount(pysparse, 1);
        Condition<List<Tuple>> sparseTupleOut = tester.tupleContents(pysparse);
        
        Condition<Long> expectedCountSparseMap = tester.tupleCount(pysparsemap, 1);
        Condition<List<Tuple>> sparseTupleMapOut = tester.tupleContents(pysparsemap);
        
        // getConfig().put(ContextProperties.TRACING_LEVEL, TraceLevel.DEBUG);
                
        complete(tester, expectedCount.and(expectedCountSparse, expectedCountSparseMap), 20, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(expectedCountSparse.valid());
        assertTrue(expectedCountSparseMap.valid());
        
        Tuple r1 = outTuples.getResult().get(0);
        
        // optional signed integers
        // 123, null
        assertEquals(r1.getObject("oi32v"), 123);
        assertNull(r1.getObject("oi32nv"));
        
        // ["a", "b"], null
        {
        @SuppressWarnings("unchecked")
		List<RString> lr = (List<RString>) r1.getObject("olrv");
        assertEquals(2, lr.size());
        assertEquals("a", lr.get(0).getString());
        assertEquals("b", lr.get(1).getString());
        assertNull(r1.getObject("olrnv"));
        }
        
        // Sparse tuple handling - source
        assertEquals(1, sparseTupleOut.getResult().size());
        Tuple st = sparseTupleOut.getResult().get(0);
        assertEquals(37, st.getObject("a")); // set by op
        assertEquals(null, st.getObject("b")); // default as None in tuple
        assertEquals(23, st.getObject("c")); // set by op
        assertEquals(-46, st.getObject("d")); // set by op
        assertEquals(null, st.getObject("e")); // default as None in tuple
        assertEquals(56, st.getObject("f")); // set by op
        assertEquals(67, st.getObject("g")); // set by op
        assertEquals(78, st.getObject("h")); // set by op
        assertEquals(null, st.getObject("i")); // default as no value (short tuple)
        
        // Sparse tuple handling - map
        assertEquals(1, sparseTupleMapOut.getResult().size());
        Tuple stm = sparseTupleMapOut.getResult().get(0);
        assertEquals(37+81, stm.getObject("a")); // set by op
        assertEquals(null, stm.getObject("b")); // match input as None in tuple
        assertEquals(23, stm.getObject("c")); // match input as None in tuple
        assertEquals(-46, stm.getObject("d")); // default to matching input
        assertEquals(null, stm.getObject("e")); // default to matching input
        assertEquals(0, stm.getObject("f")); // no match: opt to non-opt
        assertEquals(67, stm.getObject("g")); // match non-opt to non-opt
        assertEquals(78, stm.getObject("h")); // match non-opt to opt
        assertEquals(null, stm.getObject("i")); // default to matching input
        assertEquals(null, stm.getObject("j")); // default as no value (short tuple)
    }

    /**
     * Test that specific values in Python for optional types
     * make their way into SPL correctly
     * when returning as a dictionary.
     * @throws Exception
     */
    @Test
    public void testReturnDictForOptionalTypes() throws Exception {
        assumeOptionalTypes();
        Topology topology = new Topology("testReturnDictForOptionalTypes");
        
        addTestToolkit(topology);
               
        StreamSchema schema = Type.Factory.getStreamSchema("tuple<optional<int32> a, optional<int32> b, optional<int32> c, optional<int32> d, optional<int32> e>");
        
        SPLStream pyds = SPL.invokeSource(topology,
        		"com.ibm.streamsx.topology.pytest.pysource.opttype::DictTuple",
        		null, schema);
        
        SPLStream pydm = SPL.invokeOperator("com.ibm.streamsx.topology.pytest.pymap.opttype::DictTupleMap",
        		pyds, schema.extend("optional<int32>", "f"), null);
            
        Tester tester = topology.getTester();
        Condition<?> expectedCount = tester.tupleCount(pyds, 4).and(tester.tupleCount(pydm, 4));
        Condition<List<Tuple>> outTuples = tester.tupleContents(pyds);
        Condition<List<Tuple>> outTuplesMap = tester.tupleContents(pydm);
                      
        complete(tester, expectedCount, 20, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());  
        
        // Dict tuple handling - source
        Tuple r1 = outTuples.getResult().get(0);
        assertEquals(3245, r1.getObject("a"));
        assertEquals(null, r1.getObject("b"));
        assertEquals(93, r1.getObject("c"));
        assertEquals(1234, r1.getObject("d"));
        assertEquals(null, r1.getObject("e"));
        
        Tuple r2 = outTuples.getResult().get(1);
        assertEquals(831, r2.getObject("a"));
        assertEquals(421, r2.getObject("b"));
        assertEquals(null, r2.getObject("c"));
        assertEquals(-4455, r2.getObject("d"));
        assertEquals(null, r2.getObject("e"));
        
        Tuple r3 = outTuples.getResult().get(2);
        assertEquals(1, r3.getObject("a"));
        assertEquals(2, r3.getObject("b"));
        assertEquals(3, r3.getObject("c"));
        assertEquals(4, r3.getObject("d"));
        assertEquals(5, r3.getObject("e"));
        
        Tuple r4 = outTuples.getResult().get(3);
        assertEquals(null, r4.getObject("a"));
        assertEquals(-32, r4.getObject("b"));
        assertEquals(null, r4.getObject("c"));
        assertEquals(null, r4.getObject("d"));
        assertEquals(-64, r4.getObject("e"));
        
        // Now the map
        Tuple m1 = outTuplesMap.getResult().get(0);
        assertEquals(3245, m1.getObject("a"));
        assertEquals(120, m1.getObject("b"));
        assertEquals(93, m1.getObject("c"));
        assertEquals(1234, m1.getObject("d"));
        assertEquals(null, m1.getObject("e"));
        assertEquals(null, m1.getObject("f"));
        
        Tuple m2 = outTuplesMap.getResult().get(1);
        assertEquals(1, m2.getObject("a"));
        assertEquals(2, m2.getObject("b"));
        assertEquals(3, m2.getObject("c"));
        assertEquals(4, m2.getObject("d"));
        assertEquals(5, m2.getObject("e"));
        assertEquals(null, m1.getObject("f"));
        
        Tuple m3 = outTuplesMap.getResult().get(2);
        assertEquals(1, m3.getObject("a"));
        assertEquals(2, m3.getObject("b"));
        assertEquals(23, m3.getObject("c"));
        assertEquals(4, m3.getObject("d"));
        assertEquals(25, m3.getObject("e"));
        assertEquals(null, m1.getObject("f"));
        
        Tuple m4 = outTuplesMap.getResult().get(3);
        assertEquals(null, m4.getObject("a"));
        assertEquals(-39, m4.getObject("b"));
        assertEquals(null, m4.getObject("c"));
        assertEquals(null, m4.getObject("d"));
        assertEquals(-64, m4.getObject("e"));
        assertEquals(null, m1.getObject("f"));
    }
    
    private final StreamSchema INT32_SCHEMA =
            Type.Factory.getStreamSchema("tuple<int32 a>");
    
    @Test
    public void testGoodSchema() throws Exception {
        // Just verify that _testSchemaBuild works with a good schema.
        _testSchemaBuild(INT32_SCHEMA, INT32_SCHEMA);
    }
    
    @Test(expected=Exception.class)
    public void testNonHashableSetInput() throws Exception {
        StreamSchema bad = Type.Factory.getStreamSchema("tuple<set<list<int32>> a>");
        _testSchemaBuild(bad, INT32_SCHEMA);
    }
    
    @Test(expected=Exception.class)
    public void testNonHashableMapInput() throws Exception {
        StreamSchema bad = Type.Factory.getStreamSchema("tuple<map<set<int32>,rstring> a>");
        _testSchemaBuild(bad, INT32_SCHEMA);
    }
    @Test(expected=Exception.class)
    public void testNonHashableSetOutput() throws Exception {
        StreamSchema bad = Type.Factory.getStreamSchema("tuple<set<list<int32>> a>");
        _testSchemaBuild(INT32_SCHEMA, bad);
    }
    
    @Test(expected=Exception.class)
    public void testNonHashableMapOutput() throws Exception {
        StreamSchema bad = Type.Factory.getStreamSchema("tuple<map<set<int32>,rstring> a>");
        _testSchemaBuild(INT32_SCHEMA, bad);
    }
    
    /**
     * Just create a Beacon feeding a Python Noop to allow
     * checking of input and output schemas.
     */
    private void _testSchemaBuild(StreamSchema input, StreamSchema output) throws Exception{
        Topology topology = new Topology("testNonHashableSet");
        
        
        SPLStream s = SPL.invokeSource(topology, "spl.utility::Beacon", Collections.emptyMap(), input);
                
        addTestToolkit(topology);
        SPL.invokeOperator("com.ibm.streamsx.topology.pysamples.positional::Noop", s, output, null);
        
        File bundle = (File) StreamsContextFactory.getStreamsContext(StreamsContext.Type.BUNDLE).submit(topology).get();
        bundle.delete();
    }
    
    
}
