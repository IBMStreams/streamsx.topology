package games;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.streams.BeaconStreams;

/**
 * Play
 * <a href="https://en.wikipedia.org/wiki/Fizz_buzz">Fizz Buzz</a>.
 *
 */
public class FizzBuzz {

    /**
     * Entry point for a streaming Fizz Buzz!
     */
    public static void main(String[] args) throws Exception {
        Topology topology = new Topology();
        
        // Declare an infinite stream of Long values
        TStream<Long> counting = BeaconStreams.longBeacon(topology);
        
        // Throttle the rate to allow the output to be seen easier
        counting = counting.throttle(100, TimeUnit.MILLISECONDS);
        
        // Print the tuples to standard output
        playFizzBuzz(counting).print();
        
        // At this point the streaming topology (streaming) is
        // declared, but no data is flowing. The topology
        // must be submitted to a StreamsContext to be executed.

        // Since this is an streaming graph with an endless
        // data source it will run for ever
        Future<?> runningTopology = StreamsContextFactory.getEmbedded().submit(topology);
        
        // Run for one minute before canceling.
        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
        
        runningTopology.cancel(true);       
    }
    
    /**
     * Return a stream that plays Fizz Buzz based
     * upon the values in the input stream.
     */
    public static TStream<String> playFizzBuzz(TStream<Long> counting) {
        
        /*
         * Transform an input stream of longs TStream<Long> to a
         * stream of strings TStream<String> that follow
         * the Fizz Buzz rules based upon each value in the
         * input stream.
         */
        TStream<String> shouts = counting.transform(new Function<Long,String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String apply(Long v) {
                // Skip 0, humans count from 1!
                if (v == 0)
                    return null;
                
                StringBuilder sb = new StringBuilder();
                if (v % 3 == 0)
                    sb.append("Fizz");
                if (v % 5 == 0)
                    sb.append("Buzz");
                
                if (sb.length() == 0)
                    sb.append(Long.toString(v));
                else
                    sb.append("!");
                
                return sb.toString();
            }});
        
        return shouts;
    }

}
