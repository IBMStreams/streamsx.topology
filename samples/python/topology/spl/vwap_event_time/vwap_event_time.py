# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
import os
import streamsx.topology.context as context
from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ContextTypes,ConfigParams
from streamsx.topology.schema import StreamSchema
import streamsx.spl.op as op
import streamsx.spl.types
import streamsx.standard.files as files
from streamsx.standard import Compression, Format
import streamsx.standard.relational as R

script_dir = os.path.dirname(os.path.realpath(__file__))

def data_source(topo, schema):
    input_file = 'TradesAndQuotes.csv.gz'    
    sample_file = os.path.join(script_dir, input_file)
    topo.add_file_dependency(sample_file, 'etc') # add sample file to etc dir in bundle
    fn = os.path.join('etc', input_file) # file name relative to application dir
    s = topo.source(files.CSVReader(schema=schema, file=fn, compression=Compression.gzip.name))
    # add event-time
    TQRecTWithEvTime = StreamSchema(schema).extend(StreamSchema('tuple<timestamp evTime>'))
    fo = R.Functor.map(s, TQRecTWithEvTime)     
    fo.evTime = fo.output(fo.outputs[0], op.Expression.expression('timeStringToTimestamp(date, time, false)'))
    ev_stream = fo.outputs[0]
    ev_stream = ev_stream.set_event_time('evTime')
    return ev_stream

def data_sink(stream):
    fsink_config = {
       'format': Format.txt.name
    }
    fsink = files.FileSink(file=streamsx.spl.op.Expression.expression('"'+script_dir+'/out.txt"'), **fsink_config)
    stream.for_each(fsink)


def main():
    """
    This demonstrates the invocation of SPL operators from
    the SPL standard toolkit using streamsx.standard package.

    Computes the volume-weighted average price (VWAP) for a stream of stock 
    transactions. Given trades and quotes, this application produces a 
    bargain index for a given list of stocks.  The WVAP is calculated over 
    event-time intervals of 10 seconds and is calculated every second. 
    The average is not recalculated if late trades are received (tuples
    are ignored).

    The bargain index identifies possible trade opportunities, which occur 
    when the volume-weighted average price for trades executed within the 
    last 10 seconds exceeds the current ask price. This algorithm may be 
    used to identify opportunities to pick up well priced shares in the 
    stream of current transaction data.
 
    Given an input file containing trades and quotes, this application 
    produces an output file called `out.txt` in the script's directory.
    The output file lists the VWAP and bargain index.

    The bargain index identifies the magnitude of the bargain, where a greater 
    values implies a better bargain. A value of 0 indicates that the VWAP is 
    not greater than the asking price, and is therefore not a bargain.

    A few records from the output file are show below:

       {ticker="BK",vwap=32.51,askprice=32.51,asksize=50,date="27-DEC-2005",time="14:30:17.098",index=0}
       {ticker="BK",vwap=32.51,askprice=32.51,asksize=48,date="27-DEC-2005",time="14:30:17.100",index=0}
       {ticker="IBM",vwap=83.47991935483871,askprice=83.46,asksize=10,date="27-DEC-2005",time="14:30:17.238",index=10.20119069042564}
       {ticker="IBM",vwap=83.47991935483871,askprice=83.46,asksize=10,date="27-DEC-2005",time="14:30:17.238",index=10.20119069042564}
 
    Each record shows the content of the tuples received on the `bargain_index_stream` stream as a set of tuple attributes. The attributes are formatted as a key-value pair. 
    The `vwap` and `index` attributes contains the value-weighted average price and bargain index, respectively.    
            
    Example::

       python3 vwap_event_time.py

    Output:
      Bargain Index reports in file "out.txt"
    """
    topo = Topology(name='VwapEventTime')
   
    # schema definitions 
    TQRecT = 'tuple<rstring ticker,rstring date, rstring time, int32 gmtOffset, rstring ttype, rstring exCntrbID, decimal64 price, decimal64 volume, decimal64 vwap, rstring buyerID, decimal64 bidprice, decimal64 bidsize, int32 numbuyers, rstring sellerID, decimal64 askprice, decimal64 asksize, int32 numsellers, rstring qualifiers, int32 seqno, rstring exchtime, decimal64 blockTrd, decimal64 floorTrd, decimal64 PEratio, decimal64 yield, decimal64 newprice, decimal64 newvol, int32 newseqno, decimal64 bidimpvol, decimal64 askimpcol, decimal64 impvol>'
    TradeInfoT = 'tuple<decimal64 price, decimal64 volume, rstring date, rstring time, timestamp evTime, rstring ticker>'
    QuoteInfoT = 'tuple<decimal64 bidprice, decimal64 askprice, decimal64 asksize, rstring date, rstring time, timestamp evTime, rstring ticker>'
    VwapT = 'tuple<rstring ticker, decimal64 minprice, decimal64 maxprice, decimal64 avgprice, decimal64 vwap, timestamp start, timestamp end>'
    BargainIndexT = 'tuple<rstring ticker, decimal64 vwap, decimal64 askprice, decimal64 asksize, rstring date, rstring time, decimal64 index>'

    trade_quote_eventtime_stream = data_source(topo, TQRecT)

    # split quotes and trades
    fq = R.Functor.map(trade_quote_eventtime_stream, StreamSchema(QuoteInfoT), filter='ttype=="Quote" && (ticker in {"BK", "IBM", "ANR"})', name='QuoteFilter')
    quotes_stream = fq.outputs[0]

    ft = R.Functor.map(trade_quote_eventtime_stream, StreamSchema(TradeInfoT), filter='ttype=="Trade" && (ticker in {"BK", "IBM", "ANR"})', name='TradeFilter')
    trades_stream = ft.outputs[0]

    # Aggregation over event-time intervals of 10 seconds, calculated every second
    w = trades_stream.time_interval(interval_duration=10.0, creation_period=1.0).partition('ticker')
    aggregate_schema = StreamSchema(VwapT).extend(StreamSchema('tuple<decimal64 sumvolume>'))
    a = R.Aggregate.invoke(w, aggregate_schema, name='PreVwap')
    a.vwap = a.sum('price * volume')
    a.minprice = a.min('price')
    a.maxprice = a.max('price')
    a.avgprice = a.average('price')
    a.sumvolume = a.sum('volume')
    a.start = a.interval_start()
    a.end = a.interval_end()
    pre_vwap_stream = a.stream

    f_vwap = R.Functor.map(pre_vwap_stream, StreamSchema(VwapT), name='Vwap')
    f_vwap.vwap = f_vwap.output(f_vwap.outputs[0], 'vwap / sumvolume')
    vwap_stream = f_vwap.outputs[0]

    # Join quotes with an event-time up to one second greater than the VWAP time */
    win_vwap = vwap_stream.last(size=100).partition('ticker')
    j = R.Join.lookup(
       reference=win_vwap,
       reference_key='ticker',
       lookup=quotes_stream,
       lookup_key='ticker',
       schema=BargainIndexT,
       match='(Vwap.end <= QuoteFilter.evTime) && (QuoteFilter.evTime < add(Vwap.end, (float64)1.0))',
       name='BargainIndex')
    j.index = j.output(j.outputs[0], 'vwap > askprice ? asksize * exp(vwap - askprice) : 0d')
    bargain_index_stream = j.outputs[0]

    # Write data in output file
    data_sink(bargain_index_stream)

    
    # Now execute the topology by submitting to a standalone context.
    submit(ContextTypes.STANDALONE, topo)
     
if __name__ == '__main__':
    main()
    
