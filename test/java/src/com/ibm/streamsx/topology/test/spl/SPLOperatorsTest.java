/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.spl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streams.flow.handlers.MostRecent;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SubmissionParameter;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class SPLOperatorsTest extends TestTopology {
    
    /**
     * Test we can invoke an SPL operator.
     */
    @Test
    public void testSPLOperator() throws Exception {
        
        // Invokes an SPL operator so cannot run in embedded.       
        assumeSPLOk();   
        
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
    public void testLiterals() throws Exception {
        
        // Invokes an SPL operator so cannot run in embedded.       
        assumeSPLOk();   
        
        Topology topology = new Topology("testSPLParameters"); 
        
        StreamSchema schema = Type.Factory.getStreamSchema(
                "tuple<rstring r, int8 i8, int16 i16, int32 i32, int64 i64, float32 f32, float64 f64>");
        
        // Filter on the vi attribute, passing the value 321.
        Map<String,Object> params = new HashMap<>();
        
        Random rand = new Random();
        String r = "test\"Lit\nerals\\n" + rand.nextInt();
        params.put("r", r);
        
        byte i8 = (byte) rand.nextInt();
        short i16 = (short) rand.nextInt(); 
        int i32 = rand.nextInt();
        long i64 = rand.nextLong(); 
        
        
        params.put("i8", i8);
        params.put("i16", i16); 
        params.put("i32", i32); 
        params.put("i64", i64); 
        
        float f32 = rand.nextFloat();
        double f64 = rand.nextDouble();
        params.put("f32", f32); 
        params.put("f64", f64);
   
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtk"));
        SPLStream paramTuple = SPL.invokeSource(topology, "testgen::TypeLiteralTester", params, schema);

        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.tupleCount(paramTuple, 1);
        MostRecent<Tuple> mr = tester.splHandler(paramTuple, new MostRecent<Tuple>());

        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
        Tuple tuple = mr.getMostRecentTuple();
        
        assertEquals(r, tuple.getString("r"));
        assertEquals(i8, tuple.getByte("i8"));
        assertEquals(i16, tuple.getShort("i16"));
        assertEquals(i32, tuple.getInt("i32"));
        assertEquals(i64, tuple.getLong("i64"));
        assertEquals(f32, tuple.getFloat("f32"), 0.001);
        assertEquals(f64, tuple.getDouble("f64"), 0.001);
    }

    @Test
    public void testSubmissionParameterDefault() throws Exception {
        
        // Invokes an SPL operator so cannot run in embedded.       
        assumeSPLOk();   
        
        Topology topology = new Topology("testSubmissionParameterDefault"); 
        
        StreamSchema schema = Type.Factory.getStreamSchema(
                "tuple<"
                + "rstring r"
// not supported by TypeLiteralTester
//                + ", ustring u"
//                + ", boolean b"
                + ", int8 i8, int16 i16, int32 i32, int64 i64"
// not supported by TypeLiteralTester
//                + ", uint8 ui8, uint16 ui16, uint32 ui32, uint64 ui64"
                + ", float32 f32, float64 f64"
                + " >");
        
        Map<String,Object> params = new HashMap<>();
        
        Random rand = new Random();

        String r = "test \"Submission Parameters\"" + rand.nextInt();
        addParamDefault("r", r, params);
//        String u = "test \"Submission Parameters\"" + rand.nextInt();
//        addParamDefault("u", u, params);

//        boolean bool = rand.nextBoolean();
//        addParamDefault("b", bool, params);
      
        
        byte i8 = (byte) rand.nextInt();
        short i16 = (short) rand.nextInt(); 
        int i32 = rand.nextInt();
        long i64 = rand.nextLong(); 
        addParamDefault("i8", i8, params);
        addParamDefault("i16", i16, params);
        addParamDefault("i32", i32, params);
        addParamDefault("i64", i64, params);
        
//        byte ui8 = (byte) rand.nextInt();
//        short ui16 = (short) rand.nextInt(); 
//        int ui32 = rand.nextInt();
//        long ui64 = rand.nextLong(); 
//        ui8 = -1;
//        ui16 = -1;
//        ui32 = -1;
//        ui64 = -1;   
//        addUnsignedParamDefault("ui8", ui8, params);
//        addUnsignedParamDefault("ui16", ui16, params);
//        addUnsignedParamDefault("ui32", ui32, params);
//        addUnsignedParamDefault("ui64", ui64, params);
        
        float f32 = rand.nextFloat();
        double f64 = rand.nextDouble();
        addParamDefault("f32", f32, params);
        addParamDefault("f64", f64, params);
        
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtk"));
        SPLStream paramTuple = SPL.invokeSource(topology, "testgen::TypeLiteralTester", params, schema);

        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.tupleCount(paramTuple, 1);
        MostRecent<Tuple> mr = tester.splHandler(paramTuple, new MostRecent<Tuple>());

        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
        Tuple tuple = mr.getMostRecentTuple();
        
//        assertEquals(bool, tuple.getBoolean("bool"));
        assertEquals(r, tuple.getString("r"));
//        assertEquals(u, tuple.getString("u"));
        
        assertEquals(i8, tuple.getByte("i8"));
        assertEquals(i16, tuple.getShort("i16"));
        assertEquals(i32, tuple.getInt("i32"));
        assertEquals(i64, tuple.getLong("i64"));
        
//        assertEquals(ui8, tuple.getByte("ui8"));
//        assertEquals(ui16, tuple.getShort("ui16"));
//        assertEquals(ui32, tuple.getInt("ui32"));
//        assertEquals(ui64, tuple.getLong("ui64"));

        assertEquals("f32="+f32, f32, tuple.getFloat("f32"), 0.001);
        assertEquals("f64="+f64, f64, tuple.getDouble("f64"), 0.001);
    }

    @Test
    public void testSubmissionParameter() throws Exception {
        
        // Invokes an SPL operator so cannot run in embedded.       
        assumeSPLOk();   
        
        Topology topology = new Topology("testSubmissionParameter"); 
        
        StreamSchema schema = Type.Factory.getStreamSchema(
                "tuple<"
                + "rstring r"
// not supported by TypeLiteralTester
//                + ", ustring u"
//                + ", boolean b"
                + ", int8 i8, int16 i16, int32 i32, int64 i64"
// not supported by TypeLiteralTester
//                + ", uint8 ui8, uint16 ui16, uint32 ui32, uint64 ui64"
                + ", float32 f32, float64 f64"
                + " >");
        
        Map<String,Object> params = new HashMap<>();
        
        Map<String,Object> submitParams = new HashMap<>();
        
        Random rand = new Random();

        String r = "test \"Submission Parameters\"" + rand.nextInt();
        addParam("r", String.class, r, params, submitParams);
//        String u = "test \"Submission Parameters\"" + rand.nextInt();
//        addParam("u", String.class, u, params, submitParams);

//      boolean bool = rand.nextBoolean();
//      addParam("b", Boolean.class, bool, params, submitParams);
      
        
        byte i8 = (byte) rand.nextInt();
        short i16 = (short) rand.nextInt(); 
        int i32 = rand.nextInt();
        long i64 = rand.nextLong(); 
        addParam("i8", Byte.class, i8, params, submitParams);
        addParam("i16", Short.class, i16, params, submitParams);
        addParam("i32", Integer.class, i32, params, submitParams);
        addParam("i64", Long.class, i64, params, submitParams);
        
//        byte ui8 = (byte) rand.nextInt();
//        short ui16 = (short) rand.nextInt(); 
//        int ui32 = rand.nextInt();
//        long ui64 = rand.nextLong();
//        ui8 = -1;
//        ui16 = -1;
//        ui32 = -1;
//        ui64 = -1;   
//        addUnsignedParam("ui8", Byte.class, i8, params, submitParams);
//        addUnsignedParam("ui16", Short.class, i16, params, submitParams);
//        addUnsignedParam("ui32", Integer.class, i32, params, submitParams);
//        addUnsignedParam("ui64", Long.class, i64, params, submitParams);
        
        float f32 = rand.nextFloat();
        double f64 = rand.nextDouble();
        addParam("f32", Float.class, f32, params, submitParams);
        addParam("f64", Double.class, f64, params, submitParams);
        
        getConfig().put(ContextProperties.SUBMISSION_PARAMS, submitParams);
   
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtk"));
        SPLStream paramTuple = SPL.invokeSource(topology, "testgen::TypeLiteralTester", params, schema);

        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.tupleCount(paramTuple, 1);
        MostRecent<Tuple> mr = tester.splHandler(paramTuple, new MostRecent<Tuple>());

        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
        Tuple tuple = mr.getMostRecentTuple();
        
//        assertEquals(bool, tuple.getBoolean("bool"));
        assertEquals(r, tuple.getString("r"));
//        assertEquals(u, tuple.getString("u"));
        
        assertEquals(i8, tuple.getByte("i8"));
        assertEquals(i16, tuple.getShort("i16"));
        assertEquals(i32, tuple.getInt("i32"));
        assertEquals(i64, tuple.getLong("i64"));
        
//        assertEquals(ui8, tuple.getByte("ui8"));
//        assertEquals(ui16, tuple.getShort("ui16"));
//        assertEquals(ui32, tuple.getInt("ui32"));
//        assertEquals(ui64, tuple.getLong("ui64"));

        assertEquals("f32="+f32, f32, tuple.getFloat("f32"), 0.001);
        assertEquals("f64="+f64, f64, tuple.getDouble("f64"), 0.001);
    }

    static <T> void addParamDefault(String name, T defaultVal, Map<String,Object> params)
    {
        SubmissionParameter<T> sp = new SubmissionParameter<T>(name, defaultVal);
        params.put(name, sp);
    }

    static <T> void addUnsignedParamDefault(String name, T defaultVal, Map<String,Object> params)
    {
        SubmissionParameter<T> sp = SubmissionParameter.newUnsigned(name, defaultVal);
        params.put(name, sp);
    }

    static <T> void addParam(String name, Class<T> valueClass, Object submitVal, Map<String,Object> params,
            Map<String,Object> submitParams)
    {
        SubmissionParameter<T> sp = new SubmissionParameter<T>(name, valueClass);
        params.put(name, sp);
        submitParams.put(name, submitVal);
    }

    static <T> void addUnsignedParam(String name, Class<T> valueClass, Object submitVal, Map<String,Object> params,
            Map<String,Object> submitParams)
    {
        SubmissionParameter<T> sp = SubmissionParameter.newUnsigned(name, valueClass);
        params.put(name, sp);
        submitParams.put(name, submitVal);
    }
}
