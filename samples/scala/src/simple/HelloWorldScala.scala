package simple

import com.ibm.streamsx.topology.Topology
import com.ibm.streamsx.topology.streams.BeaconStreams
import com.ibm.streamsx.topology.context.StreamsContextFactory

import java.util.concurrent.TimeUnit

import com.ibm.streamsx.topology.functions.FunctionConversions._

object HelloWorldScala {
  def main(args: Array[String]) {
    val topology = new Topology("HelloWorldScala")

    var hw = topology.strings("Hello", "World!")    
    hw.print()

   StreamsContextFactory.getStreamsContext("EMBEDDED").submit(topology).get()
  }
}
