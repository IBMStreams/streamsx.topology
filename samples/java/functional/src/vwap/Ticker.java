/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package vwap;

import java.io.Serializable;

import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.tuple.Keyable;

public abstract class Ticker implements Keyable<String>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final String ticker;

    Ticker(Tuple tuple) {
        this(tuple.getString("ticker"));
    }

    Ticker(String ticker) {
        this.ticker = ticker;
    }

    Ticker(Ticker ticker) {
        this.ticker = ticker.getTicker();
    }

    @Override
    public String getKey() {
        return getTicker();
    }

    public String getTicker() {
        return ticker;
    }
}
