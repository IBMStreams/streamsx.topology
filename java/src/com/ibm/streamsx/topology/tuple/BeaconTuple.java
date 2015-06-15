/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.tuple;

import java.io.Serializable;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.streams.BeaconStreams;

/**
 * Tuple type issued by a beacon stream.
 * 
 * @see BeaconStreams
 */
public class BeaconTuple implements Serializable, Keyable<Long>, Comparable<BeaconTuple>, JSONAble {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final long sequence;
    private final long time;

    /**
     * Create a BeaconTuple with the current time.
     * 
     * @param sequence
     *            Sequence of the tuple
     */
    public BeaconTuple(long sequence) {
        this(sequence, System.currentTimeMillis());
    }

    /**
     * Create a BeaconTuple.
     * 
     * @param sequence
     *            Sequence of the tuple
     * @param time
     *            time of the tuple
     */
    public BeaconTuple(long sequence, long time) {
        this.sequence = sequence;
        this.time = time;
    }

    /**
     * Get the sequence identifier of this tuple.
     * 
     * @return sequence identifier of this tuple
     */
    public long getSequence() {
        return sequence;
    }
    
    /**
     * Get the key (sequence identifier) of this tuple.
     * @return sequence identifier of this tuple
     * @see #getSequence()
     */
    @Override
    public Long getKey() {
        return getSequence();
    }

    /**
     * Get the time of this tuple.
     * 
     * @return time of this tuple
     */
    public long getTime() {
        return time;
    }
    
    /**
     * Creates a JSON object with two attributes:
     * <UL>
     * <li>{@code sequence} Value of {@link #getSequence()}</li>
     * <li>{@code time} Value of {@link #getTime()}</li>
     * </UL>
     * @return JSON representation of this tuple.
     */
    @Override
    public JSONObject toJSON() {
        JSONObject btjson = new JSONObject();
        btjson.put("sequence", getSequence());
        btjson.put("time", getTime());
        return btjson;
    }

    @Override
    public String toString() {
        return "{sequence=" + sequence + ", time=" + time + "}";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (sequence ^ (sequence >>> 32));
        result = prime * result + (int) (time ^ (time >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BeaconTuple other = (BeaconTuple) obj;
        if (sequence != other.sequence)
            return false;
        if (time != other.time)
            return false;
        return true;
    }

    @Override
    public int compareTo(BeaconTuple o) {
        return Long.compare(sequence, o.sequence);
    }
}
