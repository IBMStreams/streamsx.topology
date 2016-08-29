/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016 
 */
/**
 * At least once processing using consistent regions.
 * 
 * <H2>Consistent regions</H2>
 * 
 * <H3>Overview</H3>
 * 
 * Because of business requirements, some applications require that all tuples
 * in an application are processed at least once. You can use a consistent
 * region in your streams processing applications to avoid data loss due to
 * software or hardware failure and meet your requirements for at-least-once
 * processing.
 * <P>
 * A consistent region is a subgraph where the states region (the operators
 * and transformations within it) become
 * consistent by processing all the tuples within defined
 * points on a stream. This enables tuples within the subgraph to be processed
 * at least once. The consistent region is periodically drained of its current
 * tuples. All tuples in the consistent region are processed through to the end
 * of the subgraph. In-memory state of operators are automatically serialized
 * and stored on checkpoint for each of the operators in the region.
 * </P>
 * <P>
 * If any element in a consistent region fails at run time, IBM Streams detects
 * the failure and triggers the restart of the element and the reset of the
 * region. In-memory state of the region is automatically reloaded to
 * a consistent point.
 * </P>
 * <P>
 * The capability to drain the subgraph, which is coupled with start operators
 * that can replay their output streams, enables a consistent region to achieve
 * at-least-once processing.
 * </P>
 * <P>
 * A stream processing application can be defined with zero, one, or more
 * consistent regions. You can define the start of a consistent region with
 * the {@link com.ibm.streamsx.topology.TStream#setConsistent(ConsistentRegionConfig) setConsistent()}.}.
 * IBM Streams then determines
 * the scope of the consistent region automatically, but you can reduce the
 * scope of the region with {@link com.ibm.streamsx.topology.TStream#autonomous()}.
 * </P>
 * <P>
 * When a subgraph is a consistent region, IBM Streams enables the
 * operators in that region to drain and reset. When a region is draining, it
 * establishes logical divisions in the output streams of each operator in the
 * region. A drain is successful when all operators in the region establish a
 * logical division in their output streams, and when all tuples before the
 * logical division are processed in their input streams. If a drain is
 * successful, it means that all operators in the region consumed all input
 * streams up until the established logical division. While the region is
 * draining or resetting, operators in the region that completed their draining
 * or resetting cannot submit new tuples. This behavior means that the tuple
 * flow within the subgraph briefly stops while the region is draining or resetting.
 * </P>
* 
 * <H3>Start of a consistent region</H3>
 * A consistent region is started by
 * {@link com.ibm.streamsx.topology.TStream#setConsistent(ConsistentRegionConfig)}.
 * The source operator for the {@code TStream} must support logic that,
 * after a failure in the region, can replay tuples since the last checkpoint.
 * Typically the stream is a source stream that produces tuple from an external system
 * like Apache Kakfa.
 * <P>
 * The logic that produces a replayable stream must be implemented
 * using IBM Streams Java or C++ primitive operator apis.
 * <BR>
 * A functional source
 * (e.g. {@link com.ibm.streamsx.topology.Topology#source(com.ibm.streamsx.topology.function.Supplier)})
 * cannot be used as the start of a consistent region,
 * thus the stream must be from an invocation of an SPL operator through
 * {@link com.ibm.streamsx.topology.spl.SPL}, 
 * {@link com.ibm.streamsx.topology.spl.JavaPrimitive}.
 * Such an invocation may be wrapped by a Java method that provides
 * a simplified version of the invocation for application developers.
 * </P>
 * <H3>Autonomous regions</H3>
 * By default processing occurs in an autonomous region where
 * operator checkpointing and recovery from failure is
 * independent of other operators. A consistent region can be
 * ended by starting an autonomous region using
 * {@link com.ibm.streamsx.topology.TStream#autonomous()}.
 
 */
package com.ibm.streamsx.topology.consistent;