/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.spl;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class SPLOperatorsTest extends TestTopology {
    
    @Before
    public void runSpl() {
        assumeSPLOk();
    }
    
    /**
     * Test we can invoke an SPL operator.
     */
    @Test
    public void testSPLOperator() throws Exception {
        
        Topology topology = new Topology("testSPLOperator"); 
        
        SPLStream tuples = SPLStreamsTest.testTupleStream(topology);
        
        // Filter on the vi attribute, passing the value 321.
        Map<String,Object> params = new HashMap<>();
        params.put("attr", tuples.getSchema().getAttribute("vi"));
        params.put("value", 321);        
   
        SPL.addToolkit(tuples, new File(getTestRoot(), "spl/testtk"));
        SPL.addToolkitDependency(tuples, "com.ibm.streamsx.topology.testing.testtk", "0.9.9");
        
        SPLStream int32Filtered = SPL.invokeOperator("testspl::Int32Filter", tuples, tuples.getSchema(), params);

        Tester tester = topology.getTester();
                
        Condition<Long> expectedCount = tester.tupleCount(int32Filtered, 2);
        Condition<List<Tuple>> expectedTuples = tester.tupleContents(int32Filtered,
                SPLStreamsTest.TEST_TUPLES[0],
                SPLStreamsTest.TEST_TUPLES[2]
                );
        
        if (isStreamingAnalyticsRun())
            getConfig().put(ContextProperties.FORCE_REMOTE_BUILD, true);

        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
        assertTrue(expectedTuples.toString(), expectedTuples.valid());
    }
    
    /**
     * Test we can add options to sc.
     * A C++ primitive operator is used to
     * detect C++11 setting and a #defined value
     */
    @Test
    public void testSCoptionsNoOpts() throws Exception {
        _testSCoptions(null, "CPP98", "NOOPT");
    }
    @Test
    public void testSCoptionsNoOpts2() throws Exception {
        _testSCoptions(Collections.emptyList(), "CPP98", "NOOPT");
    }
    @Test
    public void testSCoptionsSingle() throws Exception {
        _testSCoptions("--c++std=c++11", "CPP11", "NOOPT");
    }
    @Test
    public void testSCoptionsSingleList() throws Exception {
        _testSCoptions(Collections.singletonList("--cxx-flags=-DSCOPT_TESTING=1"), "CPP98", "SCOPT");
    }
    @Test
    public void testSCoptionsMulti() throws Exception {
        List<String> opts = new ArrayList<>();
        opts.add("--cxx-flags=-DSCOPT_TESTING=1");
        opts.add("--c++std=c++11");
        _testSCoptions(opts, "CPP11", "SCOPT");
    }
    
    private void _testSCoptions(Object options, String e1, String e2) throws Exception {
        
        Topology topology = new Topology("testSCoptions"); 
        
        SPLStream single = SPLStreams.stringToSPLStream(
                topology.constants(Collections.singletonList("A")));
          
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtk"));
        
        TStream<String> output = SPL.invokeOperator("SCO", "testspl::ScOptionTester", single,
                single.getSchema(), Collections.emptyMap()).toStringStream();
               
        if (options != null)
            this.getConfig().put(ContextProperties.SC_OPTIONS, options);
                   
        Tester tester = topology.getTester();
        
        Condition<Long> optC = tester.tupleCount(output, 2);
        Condition<List<String>> optV = tester.stringContents(output, e1, e2);

        complete(tester, optC, 10, TimeUnit.SECONDS);

        assertTrue(optC.toString(), optC.valid());
        assertTrue(optV.toString(), optV.valid());
    }
    
    /**
     * Test we can invoke an SPL operator.
     */
    @Test
    public void testSPLOperatorMultipleOuptuts() throws Exception {
        
        Topology topology = new Topology(); 
        
        SPLStream tuples = SPLStreamsTest.testTupleStream(topology);
        
        // Filter on the vi attribute, passing the value 321.
        Map<String,Object> params = new HashMap<>();
        params.put("attr", tuples.getSchema().getAttribute("vi"));
        params.put("value", 321);        
   
        SPL.addToolkit(tuples, new File(getTestRoot(), "spl/testtk"));
        SPL.addToolkitDependency(tuples, "com.ibm.streamsx.topology.testing.testtk", "0.9.9");
        List<SPLStream> outputs = SPL.invokeOperator(
                topology,
                "testSPLOperatorMultipleOuptuts",
                "testspl::Int32FilterPF", 
                Collections.singletonList(tuples),
                Collections.nCopies(2, tuples.getSchema()),
                params);
        
        SPLStream int32Filtered = outputs.get(0);
        SPLStream int32Dropped = outputs.get(1);

        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.tupleCount(int32Dropped, 2);
        Condition<List<Tuple>> expectedTuples = tester.tupleContents(int32Filtered,
                SPLStreamsTest.TEST_TUPLES[0],
                SPLStreamsTest.TEST_TUPLES[2]
                );
        Condition<List<Tuple>> droppedTuples = tester.tupleContents(int32Dropped,
                SPLStreamsTest.TEST_TUPLES[1],
                SPLStreamsTest.TEST_TUPLES[3]
                );

        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
        assertTrue(expectedTuples.toString(), expectedTuples.valid());
        assertTrue(droppedTuples.toString(), droppedTuples.valid());
    }
    
    /**
     * Test we can invoke an SPL operator with various parameter types.
     */

    private void testOpParams(String testName, OpParamAdder opParamAdder) throws Exception {
        
        Topology topology = new Topology(testName); 
        opParamAdder.init(topology, getConfig());
        
        StreamSchema schema = Type.Factory.getStreamSchema(
                "tuple<"
                + "rstring r"
                + ", ustring u"
                + ", boolean b"
                + ", int8 i8, int16 i16, int32 i32, int64 i64"
                + ", uint8 ui8, uint16 ui16, uint32 ui32, uint64 ui64"
                + ", float32 f32, float64 f64"
                + " >");
        
        Map<String,Object> expectedValues = new HashMap<>();
        
        Random rand = new Random();
        String r = "test    X\tY\"Lit\nerals\\nX\\tY " + rand.nextInt();
        opParamAdder.put("r", r);
        String u = "test    X\tY\"Lit\nerals\\nX\\tY " + rand.nextInt();
        opParamAdder.put("u", SPL.createValue(u, MetaType.USTRING));
        
        expectedValues.put("r", new RString(r));
        expectedValues.put("u", u);

        boolean b = rand.nextBoolean();
        opParamAdder.put("b", b);
        expectedValues.put("b", b);
        
        byte i8 = (byte) rand.nextInt();
        short i16 = (short) rand.nextInt(); 
        int i32 = rand.nextInt();
        long i64 = rand.nextLong(); 
        opParamAdder.put("i8", i8);
        opParamAdder.put("i16", i16); 
        opParamAdder.put("i32", i32); 
        opParamAdder.put("i64", i64); 
        
        expectedValues.put("i8", i8);
        expectedValues.put("i16", i16); 
        expectedValues.put("i32", i32); 
        expectedValues.put("i64", i64); 

        byte ui8 = (byte) 0xFF;       // 255 => -1
        short ui16 = (short) 0xFFFE;  // 65534 => -2 
        int ui32 = 0xFFFFFFFD;        // 4294967293 => -3
        long ui64 = 0xFFFFFFFFFFFFFFFCL; // 18446744073709551612 => -4
        opParamAdder.put("ui8", SPL.createValue(ui8, MetaType.UINT8));
        opParamAdder.put("ui16", SPL.createValue(ui16, MetaType.UINT16));
        opParamAdder.put("ui32", SPL.createValue(ui32, MetaType.UINT32)); 
        opParamAdder.put("ui64", SPL.createValue(ui64, MetaType.UINT64)); 
        
        expectedValues.put("ui8", ui8);
        expectedValues.put("ui16", ui16);
        expectedValues.put("ui32", ui32);
        expectedValues.put("ui64", ui64);
        
        float f32 = 4.0f;
        double f64 = 32.0;
        opParamAdder.put("f32", f32); 
        opParamAdder.put("f64", f64);
        expectedValues.put("f32", f32); 
        expectedValues.put("f64", f64);
   
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtk"));
        SPLStream paramTuple = SPL.invokeSource(topology, "testgen::TypeLiteralTester", opParamAdder.getParams(), schema);
        
        Tuple expectedTuple = schema.getTuple(expectedValues);
        
        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.tupleCount(paramTuple, 1);
        Condition<?> contents = tester.tupleContents(paramTuple, expectedTuple);;
        complete(tester, expectedCount.and(contents), 10, TimeUnit.SECONDS);
        
        assertTrue(contents.valid());
        assertTrue(expectedCount.valid());
    }

    /**
     * Operator parameter adder.
     * Base implementation adds as Literals.
     */
    private static class OpParamAdder {
        final Map<String,Object> params = new HashMap<>();
        Topology top;
        Map<String,Object> config;
        void init(Topology top, Map<String,Object> config) {
            this.top = top;
            this.config = config;
        }
        void put(String opParamName, Object opParamValue) {
            params.put(opParamName, opParamValue);
        }
        Map<String,Object> getParams() {
            return params;
        }
    }

    @Test
    public void testParamLiterals() throws Exception {
        // Test operator parameters with literal values
        testOpParams("testParamLiterals", new OpParamAdder());
    }
    
    /**
     * Test we can invoke an SPL operator with various parameter types,
     * where the type is an optional type.
     */
    private void testOpParamsOptionalTypes(String testName, OpParamAdder opParamAdder)
        throws Exception {
        
        Topology topology = new Topology(testName); 
        opParamAdder.init(topology, getConfig());
        
        StreamSchema schema = Type.Factory.getStreamSchema(
                "tuple<"
                + "rstring r"
                + ", optional<rstring> orv"
                + ", optional<rstring> ornv"
                + ", int32 i32"
                + ", optional<int32> oi32v"
                + ", optional<int32> oi32nv"
                + " >");
        
        Map<String,Object> expectedValues = new HashMap<>();
        Random rand = new Random();
        String r = "test    X\tY\"Lit\nerals\\nX\\tY " + rand.nextInt();
        opParamAdder.put("r", r);
        expectedValues.put("r", new RString(r));
        
        String orv = "test    X\tY\"Lit\nerals\\nX\\tY " + rand.nextInt();
        opParamAdder.put("orv", orv);
        // test setting optional type to null by using null in Map
        opParamAdder.put("ornv", null);
        
        expectedValues.put("orv", new RString(orv));
        expectedValues.put("ornv", null);
              
        
        int i32 = rand.nextInt();
        opParamAdder.put("i32", i32); 
        int oi32v = rand.nextInt();
        opParamAdder.put("oi32v", oi32v); 
        // test setting optional type to null by using createNullValue() in Map
        opParamAdder.put("oi32nv", SPL.createNullValue());
        
        expectedValues.put("i32", i32);
        expectedValues.put("oi32v", oi32v);
        expectedValues.put("oi32nv", null);
   
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtkopt"));
        SPLStream paramTuple = SPL.invokeSource(topology, "testgen::TypeLiteralTester", opParamAdder.getParams(), schema);
        
        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.tupleCount(paramTuple, 1);
        Condition<?> contents = tester.tupleContents(paramTuple, schema.getTuple(expectedValues));

        complete(tester, expectedCount.and(contents), 10, TimeUnit.SECONDS);

        assertTrue(contents.valid());
        assertTrue(expectedCount.valid());
    }

    @Test
    public void testParamLiteralsOptionalTypes() throws Exception {
        // Test operator parameters with literal values for optional types
        assumeOptionalTypes();
        testOpParamsOptionalTypes("testParamLiteralsOptionalTypes", new OpParamAdder());
    }

    @Test
    public void testSubmissionParamsWithDefault() throws Exception {
        
        // Test operator parameters with submission time values with defaults
        testOpParams("testSubmissionParamsWithDefault", new OpParamAdder() {
            void put(String opParamName, Object opParamValue) {
                Supplier<?> sp;
                if (!(opParamValue instanceof JsonObject))
                    sp = top.createSubmissionParameter(opParamName, opParamValue);
                else
                    sp = SPL.createSubmissionParameter(top, opParamName, opParamValue, true);
                params.put(opParamName, sp);
            }
        });
    }

    @Test
    public void testSubmissionParamsWithoutDefault() throws Exception {
           
        // Test operator parameters with submission time values without defaults
        testOpParams("testSubmissionParamsWithoutDefault", new OpParamAdder() {
            void put(String opParamName, Object opParamValue) {
                Supplier<?> sp;
                if (!(opParamValue instanceof JsonObject))
                    sp = top.createSubmissionParameter(opParamName,
                            (Class<?>)opParamValue.getClass());
                else
                    sp = SPL.createSubmissionParameter(top, opParamName, opParamValue, false);
                params.put(opParamName, sp);
                
                @SuppressWarnings("unchecked")
                Map<String,Object> submitParams = (Map<String,Object>) config.get(ContextProperties.SUBMISSION_PARAMS);
                if (submitParams == null) {
                    submitParams = new HashMap<>();
                    config.put(ContextProperties.SUBMISSION_PARAMS, submitParams);
                }
                if (!(opParamValue instanceof JsonObject))
                    submitParams.put(opParamName, opParamValue);
                else
                    submitParams.put(opParamName, pvToStr((JsonObject)opParamValue));
            }
        });
    }
    
    private String pvToStr(JsonObject jo) {
        // A Client of the API shouldn't find itself in
        // a place to need this.  It's just an artifact of
        // the way these tests are composed plus lack of a 
        // public form of valueToString(SPL.createValue(...)).

        String type = jo.get("type").getAsString();
        if (!"__spl_value".equals(type))
            throw new IllegalArgumentException("jo " + jo);
        JsonObject value = jo.get("value").getAsJsonObject();
        String metaType = value.get("metaType").getAsString();
        JsonElement v = value.get("value");
        return v.getAsString();
    }
}
