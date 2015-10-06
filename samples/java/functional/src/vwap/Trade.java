/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package vwap;

import java.math.BigDecimal;

import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.spl.SPLStream;

public class Trade extends Ticker {
    
    /**
     * 
     */
    private static final long serialVersionUID = -5415277911844071545L;

    static BigDecimal getNumber(String v) {
        if (v.isEmpty())
            return BigDecimal.ZERO;
        return new BigDecimal(v);
    }

    BigDecimal price;
    BigDecimal volume;

    public Trade(String ticker, BigDecimal price, BigDecimal volume) {
        super(ticker);
        this.price = price;
        this.volume = volume;
    }

    public Trade(Tuple tuple) {
        super(tuple);
        price = tuple.getBigDecimal("price");
        volume = tuple.getBigDecimal("volume");
    }

    public String toString() {
        return "TRADE: " + getTicker() + " price=" + price + " volume="
                + volume;
    }
    
    /**
     * Convert a trade SPL tuple to a Trade object.
     */
    public static Trade convertToTrade(Tuple tuple) {
        if ("Trade".equals(tuple.getString("ttype")))
            return new Trade(tuple);
        return null;

    }

    /**
     * Get the stream of trades from the SPL stream.
     * The stream is keyed by the ticker symbol.
     */
    public static TStream<Trade> getTrades(SPLStream tradeQuotes) {
        return tradeQuotes.transform(Trade::convertToTrade);
    }
}
