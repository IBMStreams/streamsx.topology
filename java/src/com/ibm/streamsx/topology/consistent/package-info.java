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
 * A consistent region is started by marking a source stream as consistent using
 * {@link com.ibm.streamsx.topology.TStream#setConsistent(ConsistentRegionConfig)}.
 * The operator that is the source of the {@code TStream} must support logic that,
 * after a failure in the region, can replay tuples since the last checkpoint.
 * Typically the stream is a source stream that produces tuples from an external system
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
 * 
 * <H3>Drain-checkpoint cycle</H3>
 * When a region is triggered it is:
 * <OL>
 * <LI> <em>drained</em> of any tuple related processing for
 * all tuples seen on each stream in the region</li>
 * <LI> <em>checkpointed</em> to reflect the state of each tuple processor (operator)
 * after it has processed all tuples on its input streams (it has been <em>drained</em>).
 * </OL>
 * This drain-checkpoint cycle results in a region where all the operators are
 * consistent with having processed all tuples seen on their input streams, and the
 * region as a whole is consistent with having processed all tuples the source operator
 * has submitted.
 * <P>
 * After any failure in the region, all operators are reset to a previous consistent point
 * and then tuple processing resumes with the source operator replaying tuples since
 * the last consistent point.
 * </P>
 * <H3>At least once/exactly once processing</H3> 
 * From the point of view of an operator in the region tuple processing is effectively exactly
 * once even though tuples are replayed after a failure. This is because the consistent
 * region protocol resets each operator's state to a point before the tuples were seen for
 * the first time. Each operator forgets it has seen the replayed tuple thus it seems them
 * effectively exactly once from an operator state point of view.
 * <P>
 * At least once processing is only seen when the operator modifies external state that cannot
 * be undone during a reset. For example sending an SMS text cannot be undone, so a IBM Streams
 * application using a consistent region for monitoring and sending text alerts could result
 * in more than once text message indicating an issue.
 * <BR>
 * With coordination between the operator and the external system exactly once processing
 * is possible, depending on the capabilities of the external system, for example exactly
 * once can be achieved with database and file systems.
 * </P>
 * <H3>Functional logic in consistent regions</H3>
 * 
 * The checkpointing of functional logic is identical for
 * consistent regions and
 * {@link com.ibm.streamsx.topology.Topology#checkpointPeriod(long, java.util.concurrent.TimeUnit) autonomous checkpointing}.
 * 
 * <H4>Stateless functions</H4>
 * Stateless functions can be used in a consistent region, e.g. a stateless filter
 * like {@code t = t.filter(s -> !s.empty())} on a {@code TStream<String>}.
 * During a drain-checkpoint cycle no processing occurs related to the stateless function.
 *  
 * <H4>Stateful functions</H4>
 * Stateful functions can be used in a consistent region. During a drain-checkpoint cycle
 * the function instance will be serialized as the checkpointed state.
 * 
 * <H3>Autonomous regions</H3>
 * By default processing occurs in an autonomous region where
 * operator checkpointing and recovery from failure is
 * independent of other operators. A consistent region can be
 * ended by starting an autonomous region using
 * {@link com.ibm.streamsx.topology.TStream#autonomous()}.
 * Any processing against the stream returned by {@code autonomous()}
 * is outside of the consistent region.
 */
package com.ibm.streamsx.topology.consistent;

