/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import java.io.Serializable;

@SuppressWarnings("serial")
public class ChannelAndSequence implements Serializable {
    int channel;
    int sequence;

    public ChannelAndSequence(int channel, int sequence) {
        this.channel = channel;
        this.sequence = sequence;
    }

    public int getChannel() {
        return channel;
    }

    public int getSequence() {
        return sequence;
    }
    
    @Override
    public String toString() {
        return "CS:" + getChannel() + ":" + getSequence();
    }

}
