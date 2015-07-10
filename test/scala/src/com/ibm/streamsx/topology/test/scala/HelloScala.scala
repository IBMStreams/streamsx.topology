package com.ibm.streamsx.topology.test.scala

import com.ibm.streamsx.topology.Topology
import com.ibm.streamsx.topology.TStream

object HelloScala {
    def getStream() = {
        val topology = new Topology("HelloScala")       
        topology.strings("Hello", "Scala!")
    }
}