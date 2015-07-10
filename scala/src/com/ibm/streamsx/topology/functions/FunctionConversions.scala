package com.ibm.streamsx.topology.functions;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Function;

import com.ibm.streamsx.topology.internal.logic.WrapperFunction;

import scala.language.implicitConversions;

object FunctionConversions {
  implicit def toPredicate[T](f: (T) => Boolean) = new Predicate[T] with WrapperFunction {
      def test(v: T) = f(v)
      def getWrappedFunction() = f
  }
  implicit def toUnaryOperator[T](f: (T) => T) = new UnaryOperator[T] with WrapperFunction {
      def apply(v: T) = f(v)
      def getWrappedFunction() = f
  }
  implicit def toFunction[T,U](f: (T) => U) = new Function[T,U] with WrapperFunction {
      def apply(v: T) = f(v)
      def getWrappedFunction() = f
  }
  implicit def toConsumer[T](f: (T) => Unit) = new Consumer[T] with WrapperFunction {
      def accept(v: T) = f(v)
      def getWrappedFunction() = f
  }
  implicit def toSupplier[T](f: () => T) = new Supplier[T] with WrapperFunction {
      def get() = f()
      def getWrappedFunction() = f
  }
}
