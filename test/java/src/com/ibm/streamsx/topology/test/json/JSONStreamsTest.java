/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.flow.handlers.MostRecent;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.json.JSONSchemas;
import com.ibm.streamsx.topology.json.JSONStreams;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tuple.JSONAble;

public class JSONStreamsTest extends TestTopology {
    @BeforeClass
    public static void checkHasStreamsInstall() {
        // Requires IBM JSON4J
        assumeTrue(hasStreamsInstall());
    }
    
    private static final String QUESTION =
            "What is the answer to life, the universe & everything?";

    private static final String JSON_EXAMPLE = "{\"menu\": {\n"
            + "  \"id\": \"file\",\n" + "  \"value\": \"File\",\n"
            + "  \"popup\": {\n" + "    \"menuitem\": [\n"
            + "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n"
            + "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n"
            + "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n"
            + "    ]\n" + "  }\n" + "}}";

    @Before
    public void checkLocalFiles() {
        // Uses a StreamHandler to test - not yet supported.
        assumeTrue(!isStreamingAnalyticsRun());
    }
    
    /**
     * Convert an example JSON as a String back to a String through JSON
     * deserialization and serialization.
     */
    @Test
    public void testSimpleJson() throws Exception {
        final Topology t = new Topology("SimpleJson");
        TStream<String> example = t.strings(JSON_EXAMPLE);

        TStream<JSONObject> json = JSONStreams.deserialize(example);
        
        assertEquals(JSONObject.class, json.getTupleClass());
        assertEquals(JSONObject.class, json.getTupleType());
        
        
        TStream<String> jsonString = JSONStreams.serialize(json);
        
        assertEquals(String.class, jsonString.getTupleClass());
        assertEquals(String.class, jsonString.getTupleType());


        checkJsonOutput(JSON.parse(JSON_EXAMPLE), jsonString);
    }

    private JSONArtifact checkJsonOutput(JSONArtifact expected,
            TStream<String> jsonString) throws Exception, IOException {
        
        
        assertEquals(String.class, jsonString.getTupleClass());
        assertEquals(String.class, jsonString.getTupleType());
        
        
        SPLStream splS = SPLStreams.stringToSPLStream(jsonString);
        MostRecent<Tuple> mr = jsonString.topology().getTester()
                .splHandler(splS, new MostRecent<Tuple>());

        Condition<Long> singleTuple = jsonString.topology().getTester().tupleCount(splS, 1);
        complete(jsonString.topology().getTester(), singleTuple, 10, TimeUnit.SECONDS);

        JSONArtifact rv = JSON.parse(mr.getMostRecentTuple().getString(0));

        assertEquals(expected, rv);

        return rv;
    }

    @Test
    public void testModifyingJson() throws Exception {
        final Topology t = new Topology();

        final JSONObject value = new JSONObject();
        value.put("question",QUESTION);
        TStream<JSONObject> s = t.constants(Collections.singletonList(value));

        TStream<String> jsonString = JSONStreams.serialize(s);
        TStream<JSONObject> json = JSONStreams.deserialize(jsonString);

        TStream<JSONObject> jsonm = modifyJSON(json);

        assertFalse(value.containsKey("answer"));
        JSONObject ev = new JSONObject();
        ev.put("question", QUESTION);
        ev.put("answer", 42l);

        checkJsonOutput(ev, JSONStreams.serialize(jsonm));
    }

    @SuppressWarnings("serial")
    private static TStream<JSONObject> modifyJSON(TStream<JSONObject> json) {
        return json.modify(new UnaryOperator<JSONObject>() {

            @Override
            public JSONObject apply(JSONObject v) {
                v.put("answer", 42l);
                return v;
            }
        });
    }
    
    @Test
    public void testJSONAble() throws IOException, Exception {
        Topology topology = new Topology();
        
        final TestJSONAble value = new TestJSONAble(42,QUESTION);
        TStream<TestJSONAble> s = topology.constants(
                Collections.singletonList(value)).asType(TestJSONAble.class);

        TStream<JSONObject> js = JSONStreams.toJSON(s);
        
        JSONObject ev = new JSONObject();
        ev.put("b", QUESTION);
        ev.put("a", 42l);

        checkJsonOutput(ev, JSONStreams.serialize(js));
    }
    
    @SuppressWarnings("serial")
    public static class TestJSONAble implements Serializable, JSONAble {
        
        private final int a;
        private final String b;
        
        public TestJSONAble(int a, String b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public JSONObject toJSON() {
            JSONObject jo = new JSONObject();
            jo.put("a", a);
            jo.put("b", b);
            return jo;
        }
        
    }
    
    /**
     * Test that if the serialized value is
     * an array, it ends up wrapped in an object.
     */
    @Test
    public void testDeserializeArray() throws Exception {
        final String data = "[ 100, 500, false, 200, 400 ]";
        final Topology t = new Topology();
        TStream<String> array = t.strings(data);

        TStream<JSONObject> json = JSONStreams.deserialize(array);
        TStream<String> jsonString = JSONStreams.serialize(json);
        
        JSONArray ja = (JSONArray) JSON.parse(data);
        JSONObject jo = new JSONObject();
        jo.put("payload", ja);

        checkJsonOutput(jo, jsonString);
    }
    
    @Test
    public void testJsonSPL() throws Exception {
        final Topology t = new Topology();
        TStream<String> example = t.strings(JSON_EXAMPLE);

        TStream<JSONObject> json = JSONStreams.deserialize(example);
        
        SPLStream spl = JSONStreams.toSPL(json);
        assertEquals(JSONSchemas.JSON, spl.getSchema());
        
        TStream<JSONObject> jsonFromSPL = spl.toJSON();  
        assertEquals(JSONObject.class, jsonFromSPL.getTupleClass());
        assertEquals(JSONObject.class, jsonFromSPL.getTupleType());

        TStream<String> jsonString = JSONStreams.serialize(jsonFromSPL);

        checkJsonOutput(JSON.parse(JSON_EXAMPLE), jsonString);
    }
    
    @Test
    public void testFlatten() throws Exception {
        final Topology t = new Topology();

        final JSONObject value = new JSONObject();
        final JSONArray array = new JSONArray();
        JSONObject e1 = new JSONObject(); e1.put("val", "hello"); array.add(e1);
        JSONObject e2 = new JSONObject(); e2.put("val", "goodbye"); array.add(e2);
        JSONObject e3 = new JSONObject(); e3.put("val", "farewell"); array.add(e3);
        value.put("greetings", array);
        
        List<JSONObject> inputs = new ArrayList<>();
        inputs.add(value);
        inputs.add(new JSONObject()); // no list present
        
        JSONObject emptyList = new JSONObject();
        emptyList.put("greetings", new JSONArray());
        inputs.add(emptyList);
        
        TStream<JSONObject> s = t.constants(inputs);

        TStream<JSONObject> jsonm = JSONStreams.flattenArray(s, "greetings");
        TStream<String> output = JSONStreams.serialize(jsonm);
               
        completeAndValidate(output, 10,  e1.toString(), e2.toString(), e3.toString());
    }
    
    @Test
    public void testFlattenWithAttributes() throws Exception {
        final Topology t = new Topology();
        
        JSONObject e1 = new JSONObject(); e1.put("val", "hello"); 
        JSONObject e2 = new JSONObject(); e2.put("val", "goodbye"); e2.put("a", "def");

        final JSONObject value = new JSONObject();
        {
            value.put("a", "abc");
            final JSONArray array = new JSONArray();
            array.add(e1);
            array.add(e2);
            value.put("greetings", array);
        }

        List<JSONObject> inputs = new ArrayList<>();
        inputs.add(value);

        final JSONObject value2 = new JSONObject();
        {
            final JSONArray array2 = new JSONArray();
            array2.add(e1.clone());
            array2.add(e2.clone());
            value2.put("greetings", array2);
        }
        inputs.add(value2);
        
        
        TStream<JSONObject> s = t.constants(inputs);

        TStream<JSONObject> jsonm = JSONStreams.flattenArray(s, "greetings", "a");;
        TStream<String> output = JSONStreams.serialize(jsonm);
        
        JSONObject e1r = (JSONObject) e1.clone();
        e1r.put("a", "abc");
        assertFalse(e1.containsKey("a"));
               
        completeAndValidate(output, 10,  e1r.toString(), e2.toString(), e1.toString(), e2.toString());
    }
    
    @Test
    public void testFlattenNoObjects() throws Exception {
        final Topology t = new Topology();

        final JSONObject value = new JSONObject();
        final JSONArray array = new JSONArray();
        array.add("hello");
        value.put("greetings", array);
                
        TStream<JSONObject> s = t.constants(Collections.singletonList(value));

        TStream<JSONObject> jsonm = JSONStreams.flattenArray(s, "greetings");
        TStream<String> output = JSONStreams.serialize(jsonm);
        
        JSONObject payload = new JSONObject();
        payload.put("payload", "hello");
               
        completeAndValidate(output, 10,  payload.toString());
    }
}
