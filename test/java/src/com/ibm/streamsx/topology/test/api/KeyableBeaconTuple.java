/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import java.io.Serializable;

import com.ibm.streamsx.topology.tuple.BeaconTuple;
import com.ibm.streamsx.topology.tuple.Keyable;

@SuppressWarnings("serial")
public class KeyableBeaconTuple implements Keyable<Object>, Serializable {
    BeaconTuple tup;

    public KeyableBeaconTuple(BeaconTuple tup) {
        this.tup = tup;
    }

    public BeaconTuple getTup() {
        return tup;
    }

    @Override
    public Object getKey() {
        return tup.getSequence();
    }

}
