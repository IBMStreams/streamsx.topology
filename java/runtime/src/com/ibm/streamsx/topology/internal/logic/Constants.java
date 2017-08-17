/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.Vector;

import com.ibm.streamsx.topology.function.Supplier;

public final class Constants<T> implements Supplier<Iterable<T>> {
    private static final long serialVersionUID = 1L;
    
    private final List<T> data;
    
    public Constants(List<T> data) {
        List<T> copiedData;
        
        if (data.isEmpty())
            copiedData = Collections.emptyList();
        else if (data instanceof ArrayList || data instanceof LinkedList
                || data instanceof Stack || data instanceof Vector)
            copiedData = data;
        else {
            // Copy the data in case the list is not serializable.
            // and to ensure the contents are in the stream in
            // case it is a "smart" list.
            copiedData = new ArrayList<>(data.size());
            copiedData.addAll(data);
        }      
        
        this.data = copiedData;
    }
    @Override
    public Iterable<T> get() {
        return data;
    }
}
