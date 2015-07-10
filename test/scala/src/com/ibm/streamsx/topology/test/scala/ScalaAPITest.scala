package com.ibm.streamsx.topology.test.scala

import com.ibm.streamsx.topology.Topology
import com.ibm.streamsx.topology.TStream
import com.ibm.streamsx.topology.streams.StringStreams

import com.ibm.streamsx.topology.test.TestTopology
import org.junit.Test;

import scala.collection.JavaConversions._

import com.ibm.streamsx.topology.functions.FunctionConversions._

class ScalaAPITest extends TestTopology  {
  
  /**
   * Test a topology in Scala with String constants.
   */
  @Test
  def testHelloScala() {
    val strings = HelloScala.getStream()
    completeAndValidate(strings, 10, "Hello", "Scala!");
  }
  
  /**
   * Topology with a tuple type defined in Scala
   * and an anonymous function as a filter
   */
  @Test
  def testScalaObjectsAsTuples() {
      val topology = new Topology("EmmaCharacters")  
      
      val emma = List(new Person("Emma", 20), new Person("George", 37), new Person("Harriet", 17), new Person("Jane", 20))
      
      var peopleStream = topology.constants(emma, classOf[Person])
      
      peopleStream = peopleStream.filter((p : Person) => p.age >= 20)
      
      var strings = StringStreams.toString(peopleStream)

      completeAndValidate(strings, 10, "Emma is 20", "George is 37", "Jane is 20");
  }
}