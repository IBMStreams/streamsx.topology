/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package vwap;

import java.util.Map;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.spl.FileSPLStreams;
import com.ibm.streamsx.topology.spl.FileSPLStreams.Compression;
import com.ibm.streamsx.topology.spl.SPLStream;

import simple.Util;

public class Vwap {
    private static StreamSchema TQRecT = Type.Factory
            .getStreamSchema("tuple<rstring ticker,rstring date, rstring time, int32 gmtOffset,"
                    + "rstring ttype, rstring exCntrbID, decimal64 price,"
                    + "decimal64 volume, decimal64 vwap, rstring buyerID,"
                    + "decimal64 bidprice, decimal64 bidsize, int32 numbuyers,"
                    + "rstring sellerID, decimal64 askprice, decimal64 asksize,"
                    + "int32 numsellers, rstring qualifiers, int32 seqno,"
                    + "rstring exchtime, decimal64 blockTrd, decimal64 floorTrd,"
                    + "decimal64 PEratio, decimal64 yield, decimal64 newprice,"
                    + "decimal64 newvol, int32 newseqno, decimal64 bidimpvol,"
                    + "decimal64 askimpcol, decimal64 impvol>");

    /**
     * Run the Vwap topology, printing the bargains to stdout.
     * @param args
     */
    public static void main(String[] args) throws Exception {
        String type = args[0];
        Map<String,Object> configMap = Util.createConfig(args);

        TStream<Bargain> bargains = createVwapTopology();

        bargains.print();

        StreamsContext<?> sc = StreamsContextFactory.getStreamsContext(type);
        sc.submit(bargains.topology(), configMap).get();
    }
    
    /**
     * Create the Vwap topology, returning the resultant bargains stream.
     */
    public static TStream<Bargain> createVwapTopology() {
        Topology topology = new Topology("Vwap");

        String vwapDataFile = System.getenv("STREAMS_INSTALL")
                + "/samples/spl/application/Vwap/data/TradesAndQuotes.csv.gz";

        TStream<String> vwapDataFileName = topology.strings(vwapDataFile);
        SPLStream tradeQuotes = FileSPLStreams.csvCompressedReader(
                vwapDataFileName, TQRecT, Compression.gzip);

        // Convert the SPLStreams into TStream<T> instances,
        // unpacking the SPL Tuple into Quote and Trade objects
        TStream<Trade> trades = Trade.getTrades(tradeQuotes);
        TStream<Quote> quotes = Quote.getQuotes(tradeQuotes);

        TStream<Bargain> bargains = VwapProcessing.bargains(trades, quotes);
        
        return bargains;
    }
    
    public static TStream<Bargain> realBargains(TStream<Bargain> bargains) {
        // Add a filter for actual bargains
        return bargains.filter(new Predicate<Bargain>() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean test(Bargain bargain) {
                return bargain.isBargain();
            }
        });
    }
}
