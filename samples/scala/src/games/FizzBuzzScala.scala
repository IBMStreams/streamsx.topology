package games

import com.ibm.streamsx.topology.Topology
import com.ibm.streamsx.topology.streams.BeaconStreams
import com.ibm.streamsx.topology.context.StreamsContextFactory

import java.util.concurrent.TimeUnit

import com.ibm.streamsx.topology.functions.FunctionConversions._

object FizzBuzzScala {
  def main(args: Array[String]) {
    val topology = new Topology("FizzBuzzScala")

    var counting = BeaconStreams.longBeacon(topology)
        
    // Throttle the rate to allow the output to be seen easier
   counting = counting.throttle(100, TimeUnit.MILLISECONDS)

   var shouts = counting.transform(
       (c:java.lang.Long) => {
           if (c == 0)
                null
           else {
                var shout = ""
                if (c % 3 == 0)
                   shout = "Fizz"
                if (c % 5 == 0)
                   shout += "Buzz"

                if (shout.isEmpty())
                   c.toString()
                else
                   shout + "!"
           }
       }  , classOf[String]);

   shouts.print();

   val future = StreamsContextFactory.getStreamsContext("EMBEDDED").submit(topology)
   Thread.sleep(60*1000);
   future.cancel(true);
  }
}
