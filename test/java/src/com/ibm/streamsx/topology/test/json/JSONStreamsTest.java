/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.flow.handlers.MostRecent;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function7.UnaryOperator;
import com.ibm.streamsx.topology.json.JSONSchemas;
import com.ibm.streamsx.topology.json.JSONStreams;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tuple.JSONAble;

public class JSONStreamsTest extends TestTopology {
    
    private static final String QUESTION =
            "What is the answer to life, the universe & everything?";

    private static final String JSON_EXAMPLE = "{\"menu\": {\n"
            + "  \"id\": \"file\",\n" + "  \"value\": \"File\",\n"
            + "  \"popup\": {\n" + "    \"menuitem\": [\n"
            + "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n"
            + "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n"
            + "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n"
            + "    ]\n" + "  }\n" + "}}";

    /**
     * Convert an example JSON as a String back to a String through JSON
     * deserialization and serialization.
     */
    @Test
    public void testSimpleJson() throws Exception {
        final Topology t = new Topology("SimpleJson");
        TStream<String> example = t.strings(JSON_EXAMPLE);

        TStream<JSONObject> json = JSONStreams.deserialize(example);
        TStream<String> jsonString = JSONStreams.serialize(json);

        checkJsonOutput(JSON.parse(JSON_EXAMPLE), jsonString);
    }

    private JSONArtifact checkJsonOutput(JSONArtifact expected,
            TStream<String> jsonString) throws Exception, IOException {
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
        final Topology t = new Topology("SimpleJson");

        final JSONObject value = new JSONObject();
        value.put("question",QUESTION);
        TStream<JSONObject> s = t.constants(Collections.singletonList(value),
                JSONObject.class);

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
                Collections.singletonList(value),
                TestJSONAble.class);

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
        TStream<String> jsonString = JSONStreams.serialize(jsonFromSPL);

        checkJsonOutput(JSON.parse(JSON_EXAMPLE), jsonString);
    }
    
}
