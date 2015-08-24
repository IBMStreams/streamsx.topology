package com.ibm.streamsx.topology.builder;

import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;

public class BSubmissionParameter {
    private final String name;
    private final String defaultValue;
    private final String splType;

    /*
     * A submission time parameter specification.
     * @param name submission parameter name
     * @param splType SPL type string - e.g., "int32", "uint16"
     */
    public BSubmissionParameter(String name, String splType) {
        this(name, splType, null);
    }

    /**
     * A submission time parameter specification.
     * @param name submission parameter name
     * @param splType SPL type string - e.g., "int32", "uint16"
     * @param defaultValue default value if parameter isn't specified. may be null.
     */
    public BSubmissionParameter(String name, String splType, String defaultValue) {
        this.name = name;
        this.splType = splType;
        this.defaultValue = defaultValue;
    }
    
    public String getName() {
        return name;
    }
    
    public String getSplType() {
        return splType;
    }

    public String getDefaultValue() {
        return defaultValue;
    }
    
    JSONObject toJson() {
        OrderedJSONObject jo = new OrderedJSONObject();
        jo.put("name", name);
        jo.put("splType", splType);
        jo.put("defaultValue", defaultValue);
        return jo;
    }
}
