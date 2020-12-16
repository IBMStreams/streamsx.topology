# VwapEventTime sample

This sample is derived from the SPL sample application in `$STREAMS_INSTALL/samples/spl/feature/VwapEventTimeSample` and uses the same SPL operators to produce the same output as the SPL sample application.

## Overview

VwapEventTime is a stock trading program that calculates the 
Volume Weighted Average Price (VWAP) over event-time intervals and identifies
the quotes for which the ask price is lower than the current VWAP. For those
cases it computes a Bargain Index and reports the event in the output file.


### Build and run the sample application:

```
python3 vwap_event_time.py
```

Notes: 
  - This application reads its input from TradesAndQuotes.csv.gz and
    writes its output to out.txt
