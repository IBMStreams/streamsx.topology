/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.scala

@SerialVersionUID(1L)
class Person(name: String, val age: Int)  extends Serializable {
  
  override def toString() : String = name + " is " + age 
}
