/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package vwap;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function7.BiFunction;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.logic.Logic;

public class VwapProcessing {

    @SuppressWarnings("serial")
    public static TStream<Bargain> bargains(TStream<Trade> trades,
            TStream<Quote> quotes) {

        TStream<VWapT> vwap = trades.last(4).aggregate(
                new Function<Iterable<Trade>, VWapT>() {

                    @Override
                    public VWapT apply(Iterable<Trade> tuples) {
                        VWapT vwap = null;
                        for (Trade trade : tuples) {
                            if (vwap == null)
                                vwap = new VWapT(trade);
                            vwap.newTrade(trade);
                        }
                        return vwap == null ? null : vwap.complete();
                    }
                }, VWapT.class);

        TStream<Bargain> bargainIndex = quotes.join(vwap.last(),
                Logic.first(new BiFunction<Quote, VWapT, Bargain>() {

                    @Override
                    public Bargain apply(Quote v1, VWapT v2) {
                        return new Bargain(v1, v2);
                    }
                }), Bargain.class);

        return bargainIndex;
    }
}
