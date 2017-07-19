/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.ibm.streamsx.topology.tester.Condition;

public class MultiLongCondition implements Condition<Long> {
    private final List<Condition<Long>> conditions;

    public MultiLongCondition(List<Condition<Long>> conditions) {
        this.conditions = Collections.unmodifiableList(new ArrayList<>(conditions));
    }
    
    public List<Condition<Long>> conditions() {
        return conditions;
    }
    
    @Override
    public boolean valid() {
        for (Condition<?> c : conditions) {
            if (!c.valid())
                return false;
        }
        return true;
    }

    @Override
    public Long getResult() {
        long l = 0;
        for (Condition<Long> c : conditions) {
            l += c.getResult();
        }
        return l;
    }
    
    @Override
    public String toString() {
        return conditions.toString();
    }

    @Override
    public boolean failed() {
        for (Condition<?> c : conditions) {
            if (c.failed())
                return true;
        }
        return false;
    }

}
