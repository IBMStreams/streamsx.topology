/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.scala

import com.ibm.streamsx.topology.Topology
import com.ibm.streamsx.topology.TStream
import com.ibm.streamsx.topology.streams.StringStreams

import com.ibm.streamsx.topology.test.TestTopology
import org.junit.Test;

import scala.collection.JavaConversions._

import com.ibm.streamsx.topology.functions.FunctionConversions._

class ScalaWindowAPITest extends TestTopology {

  /**
   * Test that an aggregate can be written.
   */
  @Test
  def testScalaObjectsAggregate() {
    val topology = new Topology("EmmaCharactersAge")

    val emma = List(new Person("Emma", 20), new Person("George", 37), new Person("Harriet", 17), new Person("Jane", 20))

    var peopleStream = topology.constants(emma).asType(classOf[Person])

    var peopleWindow = peopleStream.last(3);

    // Have to use explicit Java list!
    var oldestPerson = peopleWindow.aggregate((people: java.util.List[Person]) => {
      var oldest: Person = new Person("", -1)

      people.foreach { person =>
        if (person.age > oldest.age)
          oldest = person
      }
      oldest
    })

    var strings = StringStreams.toString(oldestPerson)

    completeAndValidate(strings, 10, "Emma is 20", "George is 37", "George is 37", "George is 37");
  }
}
