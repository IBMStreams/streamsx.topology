/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package vwap;

import static java.math.BigDecimal.ZERO;
import static java.math.MathContext.DECIMAL64;

import java.math.BigDecimal;

public class VWapT extends Ticker {
    private static final long serialVersionUID = 1L;
    BigDecimal minPrice;
    BigDecimal maxPrice;
    BigDecimal avgPrice = ZERO;
    BigDecimal vwap = ZERO;

    transient BigDecimal totalVolume = ZERO;
    transient int count;

    VWapT(Trade trade) {
        super(trade);
    }

    void newTrade(Trade trade) {

        BigDecimal price = trade.price;
        BigDecimal volume = trade.volume;

        if (ZERO.equals(volume))
            return;

        minPrice = (minPrice == null) ? price : minPrice.min(price);
        maxPrice = (maxPrice == null) ? price : maxPrice.max(price);
        avgPrice = avgPrice.add(price, DECIMAL64);
        totalVolume = totalVolume.add(volume, DECIMAL64);
        vwap = vwap.add(price.multiply(volume, DECIMAL64), DECIMAL64);
        count++;
    }

    VWapT complete() {
        if (count == 0)
            return null;
        vwap = vwap.divide(totalVolume, DECIMAL64);
        avgPrice = avgPrice.divide(new BigDecimal(count), DECIMAL64);
        return this;
    }

    public String toString() {
        return "VWAP: " + getTicker() + " vwap=" + vwap + " avgPrice="
                + avgPrice + " minPrice=" + minPrice + " maxPrice=" + maxPrice;
    }
}
