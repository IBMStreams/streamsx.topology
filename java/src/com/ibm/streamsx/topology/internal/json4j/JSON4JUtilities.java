package com.ibm.streamsx.topology.internal.json4j;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.json.java.JSONObject;

public class JSON4JUtilities {

    /**
     * Convert a IBM JSON4J JSON object to a Google Gson object.
     */
    public static JsonObject gson(JSONObject object) {
        try {
            return new JsonParser().parse(object.serialize()).getAsJsonObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
