/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
/**
 * Java application API for IBM Streams.
 * 
 * This API is used to generate streaming topologies for
 * execution by IBM Streams 4.0 or later. An instance
 * of {@link com.ibm.streamsx.topology.Topology} is created
 * that then is used to build a topology (or graph) of
 * streams, represented by {@link com.ibm.streamsx.topology.TStream}
 * instances. This topology is the declaration of the application,
 * and it may then be :
 * <UL>
 * <LI>Submitted to an IBM Streams instance.</LI>
 * <LI>Executed as an IBM Streams standalone application.</LI>
 * <LI>Compiled into an IBM Streams application bundle ({@code .sab} file).</LI>
 * <LI>Executed within the local JVM (not all toppologies are supported).</LI>
 * </UL>
 * 
 * A {@link com.ibm.streamsx.topology.TStream TStream} represents a continuous stream of tuples
 * with a specific type {@code T}. Streams are transformed into
 * new streams through functional transformations, defined using
 * a single method of an interface like {@link com.ibm.streamsx.topology.function.Function}.
 * <P>
 * A transformation consists of an instance of a functional interface,
 * known as the <i>functional logic</i>
 * and how that logic is applied against tuples on the stream.
 * 
 * The method on {@code TStream} the <i>functional logic</i> is
 * passed into to create a new stream, declares how the <i>functional logic</i>
 * is applied, for example:
 * <UL>
 * <LI>
 *  {@link com.ibm.streamsx.topology.TStream#filter(com.ibm.streamsx.topology.function.Predicate)} declares that the <i>functional logic</i> will be
 *  called for each tuple on the stream, and if it returns {@code true} then the
 *  input tuple is submitted to the new stream, otherwise the input tuple is discarded.
 * </LI>
 * <LI>
 * {@link com.ibm.streamsx.topology.TStream#transform(com.ibm.streamsx.topology.function.Function)} declares that the <i>functional logic</i> will be
 *  called for each tuple and its return value will be submitted to the output stream, unless
 *  it is {@code null}, in which case no tuple will be submitted.
 * </LI>
 * </UL>
 * <h3>Functional Logic semantics</h3>
 * <h4>Declaring Functional Logic</h4>
 * When declaring (building) the topology any <i>functional logic</i> is an instance
 * of a Java interface, such as {@link com.ibm.streamsx.topology.function.Predicate}.
 * These objects are <b> always </b> serialized and deserialized before being
 * executed against tuples on streams, regardless of the type of the {@code StreamsContext}.
 * <BR>
 * <UL>
 * <LI> If the instance is declared using an anonymous class then it should be declared in
 * a static context (e.g. static method). If the anonymous class is <b>not</b> declared in a
 * static context then it will also capture the reference of the object that declares it,
 * typically this object (the code declaring the topology) will not be serializable,
 * and is not required by the anonymous class for execution.
 * </li>
 * <ul>
 * <li>
 * The code declaring the topology passes parameters into an anonymous class by
 * having local variables or method parameters that are {@code final} and thus
 * can be captured by the anonymous class.
 * </li>
 * <li>
 * The code within an anonymous class must not reference any static fields set up
 * by the code declaring the topology, as the code declaring the topology
 * is not executed when the topology itself is executed. For example a method
 * in an anonymous class that references a static file name set when
 * declaring the topology, will not see that value when executing at runtime.
 * </li>
 * </ul>
 * <li>
 * When the functional logic is deserialized before being executed at runtime,
 * its constructor is not called. Thus any transient fields either need to
 * be initialized on their first use or using the Java serialization hooks
 * {@code readObject()} or {@code readResolve()}. The sample {@code RegexGrep}
 * has an example of this.
 * </li>
 * <li>
 * A single deserialized instance of functional logic is called for all tuples on each
 * {@link com.ibm.streamsx.topology.TStream#parallel(int) parallel channel}, when the stream is not parallelized
 * a single instance is called.
 * </li>
 * </UL>
 * <h4>Synchronization of Functional Logic</h4>
 * <UL>
 * <LI>
 * Call to a functional logic are not executed concurrently. 
 * </LI>
 * <LI>
 * Each call to a functional logic <i>happens-before</i> any subsequent call to it. 
 * </LI>
 * </UL>
 * <BR>
 * Thus there is no need for a functional logic to have synchronization to protect state
 * from multiple concurrent calls or to ensure visibility.
 * 
 * <h4>Stateful Functional Logic</h4>
 * A functional logic instance lives for the lifetime of its container (embedded JVM, standalone process
 * or processing element in distributed mode),
 * thus it may maintain state across the invocations of its method. For a de-duplicating
 * {@link com.ibm.streamsx.topology.function.Predicate} may maintain a collection of
 * previously seen tuples on the stream to filter out duplicates.
 * <BR>
 * In distributed mode, if a processing element (PE) restarts then any state will be lost and
 * a new functional logic instance set to its initial deserialized state is created.
 * <BR>
 * For future compatibility:
 * <ul>
 * <li>
 * any state that should not be persisted on a checkpoint
 * must be marked as {@code transient}, such as connections to external systems.</li>
 * <li>
 * any non-changing instance fields should be marked as {@code final}.
 * </li>
 * </ul>
 * <H3>Pass by reference semantics for tuples</H3>
 * Where possible, tuples are passed by reference from one stream to another,
 * thus in a general case, a tuple (as a Java object) is returned from one
 * <i>functional logic</i> instance and passed into one or more downstream <i>functional logic</i>
 * instances, using its reference.
 * For example, with the flow:
 * <BR>
 * {@code s.transform(trans1).filter(filt).transform(trans2)}
 * <BR>
 * a tuple {@code t} returned from {@code trans1.apply(t)}
 * may be passed directly as {@code filt.test(t)} and to {@code trans2.apply(t)} if
 * the former returned {@code true}.
 * <BR>
 * When a tuple is not, or can not, be passed by reference, for example
 * when <i>functional logic</i> instances are being executed on different
 * hosts in a distributed environment, then the tuple object will passed using
 * serialization and de-serialization.
 * <H4>Ensuring consistent behavior</H4>
 * To ensure consistent behavior regardless of how the topology is executed,
 * these recommendations should be followed:
 * <UL>
 * <LI>
 * Where possible {@code TStream} types should be immutable, which means any downstream
 * <i>functional logic</i> cannot modify any tuple through its reference. Thus a
 * reference to a tuple may be safely passed to multiple downstream <i>functional logic</i>
 * instances.
 * </LI>
 * <LI>
 * Any <i>functional logic</i> discards any reference to any mutable returned item, thus any
 * change made to an mutable tuple object are not seen by any upstream <i>functional logic</i>.
 * </LI>
 * <LI>
 * Any <i>functional logic</i> discards any reference to any mutable argument when it returns
 * from its method.</i>.
 * </LI>
 * </UL>
 *  <h3>Included Libraries</h3>
 *  This API requires the IBM Streams Java Operator API
 *  which results in these libraries in the class path:
 *  <UL>
 *  <LI>
 *  <a target="_blank" href="http://www-01.ibm.com/support/knowledgecenter/SSCRJU_4.0.0/com.ibm.streams.spl-java-operators.doc/api/overview-summary.html">IBM Streams Java Operator API</a> - {@code $STREAMS_INSTALL/lib/com.ibm.streams.operator.jar}</LI>
 *  <LI>
 *  <a target="_blank" href="http://www-01.ibm.com/support/knowledgecenter/SSCRJU_4.0.0/com.ibm.streams.spl-java-operators.doc/samples/overview-summary.html">IBM Streams Java Operator samples</a> - {@code $STREAMS_INSTALL/lib/com.ibm.streams.operator.samples.jar}</LI>
 *  <LI><a target="_blank" href="http://commons.apache.org/proper/commons-math/javadocs/api-2.2/index.html">Apache Commons Math 2.2</a> - {@code $STREAMS_INSTALL/ext/lib/commons-math-2.1.jar}</LI>
 *  <LI><a target="_blank" href="https://logging.apache.org/log4j/1.2/apidocs/">Apache Log4j 1.2.17</a> - {@code $STREAMS_INSTALL/ext/lib/log4j-1.2.17.jar}</LI>
 *  <LI><a target="_blank" href="http://www-01.ibm.com/support/knowledgecenter/api/content/SS7K4U_8.5.5/com.ibm.websphere.javadoc.liberty.doc/com.ibm.websphere.appserver.api.json_1.0-javadoc/index.html">JSON4J</a> - {@code $STREAMS_INSTALL/ext/lib/JSON4J.jar}</LI>
 *  </UL>
 */
package com.ibm.streamsx.topology;

