/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.spl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
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
import com.ibm.streams.flow.handlers.MostRecent;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
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
        SPLStream int32Filtered = SPL.invokeOperator("testspl::Int32Filter", tuples, tuples.getSchema(), params);

        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.tupleCount(int32Filtered, 2);
        Condition<List<Tuple>> expectedTuples = tester.tupleContents(int32Filtered,
                SPLStreamsTest.TEST_TUPLES[0],
                SPLStreamsTest.TEST_TUPLES[2]
                );

        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
        assertTrue(expectedTuples.toString(), expectedTuples.valid());
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
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        
        StreamSchema schema = Type.Factory.getStreamSchema(
                "tuple<"
                + "rstring r"
                + ", ustring u"
                + ", boolean b"
                + ", int8 i8, int16 i16, int32 i32, int64 i64"
                + ", uint8 ui8, uint16 ui16, uint32 ui32, uint64 ui64"
                + ", float32 f32, float64 f64"
                + " >");
        
        Random rand = new Random();
        String r = "test    X\tY\"Lit\nerals\\nX\\tY " + rand.nextInt();
        opParamAdder.put("r", r);
        String u = "test    X\tY\"Lit\nerals\\nX\\tY " + rand.nextInt();
        opParamAdder.put("u", SPL.createValue(u, MetaType.USTRING));

        boolean b = rand.nextBoolean();
        opParamAdder.put("b", b);
        
        byte i8 = (byte) rand.nextInt();
        short i16 = (short) rand.nextInt(); 
        int i32 = rand.nextInt();
        long i64 = rand.nextLong(); 
        opParamAdder.put("i8", i8);
        opParamAdder.put("i16", i16); 
        opParamAdder.put("i32", i32); 
        opParamAdder.put("i64", i64); 

        byte ui8 = (byte) 0xFF;       // 255 => -1
        short ui16 = (short) 0xFFFE;  // 65534 => -2 
        int ui32 = 0xFFFFFFFD;        // 4294967293 => -3
        long ui64 = 0xFFFFFFFFFFFFFFFCL; // 18446744073709551612 => -4
        opParamAdder.put("ui8", SPL.createValue(ui8, MetaType.UINT8));
        opParamAdder.put("ui16", SPL.createValue(ui16, MetaType.UINT16));
        opParamAdder.put("ui32", SPL.createValue(ui32, MetaType.UINT32)); 
        opParamAdder.put("ui64", SPL.createValue(ui64, MetaType.UINT64)); 
        
        float f32 = rand.nextFloat();
        double f64 = rand.nextDouble();
        opParamAdder.put("f32", f32); 
        opParamAdder.put("f64", f64);
   
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtk"));
        SPLStream paramTuple = SPL.invokeSource(topology, "testgen::TypeLiteralTester", opParamAdder.getParams(), schema);

        // paramTuple.print();
        // paramTuple.filter(new AllowAll<Tuple>());
        
        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.tupleCount(paramTuple, 1);
        MostRecent<Tuple> mr = tester.splHandler(paramTuple, new MostRecent<Tuple>());

        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
        Tuple tuple = mr.getMostRecentTuple();
        // System.out.println("tuple: " + tuple);
        
        assertEquals(r, tuple.getString("r"));
        assertEquals(u, tuple.getString("u"));
        assertEquals(i8, tuple.getByte("i8"));
        assertEquals(i16, tuple.getShort("i16"));
        assertEquals(i32, tuple.getInt("i32"));
        assertEquals(i64, tuple.getLong("i64"));
        assertEquals(ui8, tuple.getByte("ui8"));
        assertEquals("255", tuple.getString("ui8"));
        assertEquals(ui16, tuple.getShort("ui16"));
        assertEquals("65534", tuple.getString("ui16"));
        assertEquals(ui32, tuple.getInt("ui32"));
        assertEquals("4294967293", tuple.getString("ui32"));
        assertEquals(ui64, tuple.getLong("ui64"));
        assertEquals("18446744073709551612", tuple.getString("ui64"));
        assertEquals(f32, tuple.getFloat("f32"), 0.001);
        assertEquals(f64, tuple.getDouble("f64"), 0.001);
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
        
        assumeTrue(!isStreamingAnalyticsRun()); // TODO: Uses Stream handler
        
        Topology topology = new Topology(testName); 
        opParamAdder.init(topology, getConfig());
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        
        StreamSchema schema = Type.Factory.getStreamSchema(
                "tuple<"
                + "rstring r"
                + ", optional<rstring> orv"
                + ", optional<rstring> ornv"
                + ", int32 i32"
                + ", optional<int32> oi32v"
                + ", optional<int32> oi32nv"
                + " >");
        
        Random rand = new Random();
        String r = "test    X\tY\"Lit\nerals\\nX\\tY " + rand.nextInt();
        opParamAdder.put("r", r);
        String orv = "test    X\tY\"Lit\nerals\\nX\\tY " + rand.nextInt();
        opParamAdder.put("orv", orv);
        // test setting optional type to null by using null in Map
        opParamAdder.put("ornv", null);
        
        int i32 = rand.nextInt();
        opParamAdder.put("i32", i32); 
        int oi32v = rand.nextInt();
        opParamAdder.put("oi32v", oi32v); 
        // test setting optional type to null by using createNullValue() in Map
        opParamAdder.put("oi32nv", SPL.createNullValue());
   
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtkopt"));
        SPLStream paramTuple = SPL.invokeSource(topology, "testgen::TypeLiteralTester", opParamAdder.getParams(), schema);

        // paramTuple.print();
        // paramTuple.filter(new AllowAll<Tuple>());
        
        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.tupleCount(paramTuple, 1);
        MostRecent<Tuple> mr = tester.splHandler(paramTuple, new MostRecent<Tuple>());

        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
        Tuple tuple = mr.getMostRecentTuple();
        // System.out.println("tuple: " + tuple);
        
        assertEquals(r, tuple.getString("r"));
        assertEquals(orv, tuple.getString("orv"));
        assertEquals(new RString(orv), tuple.getObject("orv"));
        assertEquals("null", tuple.getString("ornv"));
        assertEquals(null, tuple.getObject("ornv"));
        assertEquals(i32, tuple.getInt("i32"));
        assertEquals(new Integer(oi32v), tuple.getObject("oi32v"));
        assertEquals(null, tuple.getObject("oi32nv"));
    }

    @Test
    public void testParamLiteralsOptionalTypes() throws Exception {
        // Test operator parameters with literal values for optional types
        assumeOptionalTypes();
        testOpParamsOptionalTypes("testParamLiteralsOptionalTypes", new OpParamAdder());
    }

    @Test
    public void testSubmissionParamsWithDefault() throws Exception {
        assumeTrue(!isStreamingAnalyticsRun()); // TODO: Uses Stream handler
        
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
        assumeTrue(!isStreamingAnalyticsRun()); // TODO: Uses Stream handler
           
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
