/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.functions

import com.ibm.streamsx.topology.function.BiFunction
import com.ibm.streamsx.topology.function.Consumer
import com.ibm.streamsx.topology.function.Supplier
import com.ibm.streamsx.topology.function.UnaryOperator
import com.ibm.streamsx.topology.function.Predicate
import com.ibm.streamsx.topology.function.Function
import com.ibm.streamsx.topology.function.ToIntFunction

import com.ibm.streamsx.topology.internal.logic.WrapperFunction;

import scala.language.implicitConversions;

/**
 * Implicit function conversions for Scala to allow
 * use of the IBM Streams Java Application API.
 */
object FunctionConversions {
  /**
   * Implicit conversion for BiFunction.
   * Allows a Scala function to be passed into
   * IBM Streams Java Application API methods that
   * expect a {@code com.ibm.streamsx.topology.function.BiFunction}.
   */
    implicit def toBiFunction[T1,T2,R](f: (T1,T2) => R) = new BiFunction[T1,T2,R] with WrapperFunction {
      def apply(v1:T1, v2:T2) = f(v1,v2)
      def getWrappedFunction() = f
  }
  /**
   * Implicit conversion for Consumer.
   * Allows a Scala function to be passed into
   * IBM Streams Java Application API methods that
   * expect a {@code com.ibm.streamsx.topology.function.Consumer}.
   */
  implicit def toConsumer[T](f: (T) => Unit) = new Consumer[T] with WrapperFunction {
      def accept(v: T) = f(v)
      def getWrappedFunction() = f
  }
  /**
   * Implicit conversion for Function.
   * Allows a Scala function to be passed into
   * IBM Streams Java Application API methods that
   * expect a {@code com.ibm.streamsx.topology.function.Function}.
   */
  implicit def toFunction[T,U](f: (T) => U) = new Function[T,U] with WrapperFunction {
      def apply(v: T) = f(v)
      def getWrappedFunction() = f
  }
  /**
   * Implicit conversion for Predicate.
   * Allows a Scala function to be passed into
   * IBM Streams Java Application API methods that
   * expect a {@code com.ibm.streamsx.topology.function.Predicate}.
   */
  implicit def toPredicate[T](f: (T) => Boolean) = new Predicate[T] with WrapperFunction {
      def test(v: T) = f(v)
      def getWrappedFunction() = f
  }
  /**
   * Implicit conversion for Supplier.
   * Allows a Scala function to be passed into
   * IBM Streams Java Application API methods that
   * expect a {@code com.ibm.streamsx.topology.function.Supplier}.
   */
  implicit def toSupplier[T](f: () => T) = new Supplier[T] with WrapperFunction {
      def get() = f()
      def getWrappedFunction() = f
  }
  /**
   * Implicit conversion for UnaryOperator.
   * Allows a Scala function to be passed into
   * IBM Streams Java Application API methods that
   * expect a {@code com.ibm.streamsx.topology.function.UnaryOperator}.
   */
  implicit def toUnaryOperator[T](f: (T) => T) = new UnaryOperator[T] with WrapperFunction {
      def apply(v: T) = f(v)
      def getWrappedFunction() = f
  }
  /**
   * Implicit conversion for ToIntFunction.
   * Allows a Scala function to be passed into
   * IBM Streams Java Application API methods that
   * expect a {@code com.ibm.streamsx.topology.function.ToIntFunction}.
   */
  implicit def toToIntFunction[T](f: (T) => Int) = new ToIntFunction[T] with WrapperFunction {
      def applyAsInt(v: T) = f(v)
      def getWrappedFunction() = f
  }
}

