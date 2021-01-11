/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
 */
package com.ibm.streamsx.topology.generator.operator;

public interface WindowProperties {
    
    String TYPE_NOT_WINDOWED = "NOT_WINDOWED";
    String TYPE_SLIDING = "SLIDING";
    String TYPE_TUMBLING = "TUMBLING";
    String TYPE_TIME_INTERVAL = "TIME_INTERVAL";

    String POLICY_COUNT = "COUNT";
    String POLICY_DELTA = "DELTA";
    String POLICY_NONE = "NONE";
    String POLICY_PUNCTUATION = "PUNCTUATION";
    String POLICY_TIME = "TIME";
}
