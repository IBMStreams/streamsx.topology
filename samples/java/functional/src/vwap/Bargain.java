/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package vwap;

import java.math.BigDecimal;
import java.math.MathContext;

public class Bargain extends Ticker {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    final Quote quote;
    final VWapT vwap;
    final BigDecimal index;

    Bargain(Quote quote, VWapT vwap) {
        super(quote.getTicker());
        this.quote = quote;
        this.vwap = vwap;

        BigDecimal idx = BigDecimal.ZERO;
        if (vwap.vwap.compareTo(quote.askprice) > 0) {
            double ep = Math.pow(Math.E, vwap.vwap.subtract(quote.askprice)
                    .doubleValue());
            idx = quote.asksize.multiply(new BigDecimal(ep),
                    MathContext.DECIMAL64);
        }
        index = idx;
    }

    @Override
    public String toString() {
        return "BARGAIN: " + quote + " " + vwap + " Index: " + index;
    }
    
    public boolean isBargain() {
        return index.compareTo(BigDecimal.ZERO) > 0;
    }
}