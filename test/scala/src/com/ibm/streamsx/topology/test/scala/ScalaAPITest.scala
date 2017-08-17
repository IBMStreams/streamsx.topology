/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.scala

import com.ibm.streamsx.topology.Topology
import com.ibm.streamsx.topology.TStream
import com.ibm.streamsx.topology.streams.StringStreams

import com.ibm.streamsx.topology.test.TestTopology
import org.junit.Assert._
import org.junit.Test;

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


import com.ibm.streamsx.topology.functions.FunctionConversions._

class ScalaAPITest extends TestTopology {
  
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
      val topology = new Topology("testScalaObjectsAsTuples")  
      
      val emma = List(new Person("Emma", 20), new Person("George", 37), new Person("Harriet", 17), new Person("Jane", 20))
      
      var peopleStream = topology.constants(emma).asType(classOf[Person])
      
      peopleStream = peopleStream.filter((p : Person) => p.age >= 20)
      
      var strings = StringStreams.toString(peopleStream)

      completeAndValidate(strings, 10, "Emma is 20", "George is 37", "Jane is 20");
  }
  
    @Test
  def testScalaTransform() {
      val topology = new Topology("testScalaTransform")  
      
      val emma = List(new Person("Emma", 20), new Person("George", 37), new Person("Harriet", 17), new Person("Jane", 20))
      
      var peopleStream = topology.constants(emma).asType(classOf[Person])
      
      var strings : TStream[String] = peopleStream.transform((p : Person) => p.toString())
      
      completeAndValidate(strings, 10, "Emma is 20", "George is 37", "Harriet is 17", "Jane is 20")
  }
  
  @Test
  def testScalaSupplier() {
      val topology = new Topology("testScalaSupplier")
      
      val emma = List(new Person("Emma", 20), new Person("George", 37), new Person("Harriet", 17), new Person("Jane", 20))
      val getCharacters : () => java.util.List[Person] = () =>  { emma };  

      var supplierStream : TStream[Person] = topology.source(getCharacters)
      
      var strings = StringStreams.toString(supplierStream);
      
      completeAndValidate(strings, 10, "Emma is 20", "George is 37", "Harriet is 17", "Jane is 20")
  }
  
    @Test
  def testScalaConsumer() {
      val topology = new Topology("testScalaConsumer")
      
      val emma = List(new Person("Emma", 20), new Person("George", 37), new Person("Harriet", 17), new Person("Jane", 20))
      val getCharacters : () => java.util.List[Person] = () =>  { emma };  

      var supplierStream : TStream[Person] = topology.source(getCharacters)
      
      // just verifying we can write a consumer
      supplierStream.sink((p : Person) => { Console.println(p) });
  }
    
        @Test
    def testScalaMultiTransform() {
        val topology = new Topology("testScalaMultiTransform");
        val source = topology.strings("mary had a little lamb",
                "its fleece was white as snow");

        val words : TStream[String] = source.multiTransform((s : String) => {
                     val sseq : Seq[String] = s.split(" ")
                     sseq
                     } : java.lang.Iterable[String]);
        
        completeAndValidate(words, 10, "mary", "had",
                "a", "little", "lamb", "its", "fleece", "was", "white", "as",
                "snow");
    }
        
  @Test
  def testScalaSplit() {
      val topology = new Topology("testScalaSplit")  
      
      val emma = List(new Person("Emma", 20), new Person("George", 37), new Person("Harriet", 17), new Person("Jane", 20))
      
      var peopleStream = topology.constants(emma).asType(classOf[Person])
      
      var splitStream = peopleStream.split(2, (p : Person) => p.age);
            
      var channel0 : TStream[String] = StringStreams.toString(splitStream(0))
      var channel1 : TStream[String] = StringStreams.toString(splitStream(1))
      
      var channel1Condition = topology.getTester().stringContents(channel1, "George is 37", "Harriet is 17")
      
     completeAndValidate(channel0, 10, "Emma is 20", "Jane is 20")

     assertTrue(channel1Condition.toString(), channel1Condition.valid());
  }
}
