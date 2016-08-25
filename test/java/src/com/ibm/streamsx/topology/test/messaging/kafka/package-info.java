/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
/**
 * Tests for {@link com.ibm.streamsx.topology.messaging.kafka.KafkaConsumer KafkaConsumer}
 * and {@link com.ibm.streamsx.topology.messaging.kafka.KafkaProducer KafkaProducer}
 *
 * <p>The tests require access to an operational Kafka Cluster with the
 * following configuration:
 * <ul>
 * <li>the associated zookeeper is accessible on {@code localhost:2181}</li>
 * <li>the Kafka Cluster server is accessible on {@code localhost:9092}</li>
 * <li>the Kafka topics {@code testTopic1} and {@code testTopic2} exist</li>
 * </ul>
 * 
 * <p>See the {@link com.ibm.streamsx.topology.test.messaging.kafka.KafkaStreamsTest KafkaStreamsTest}
 * class documentation to specify different configuration properties. 
 *  
 * <p>Information on setting up a Kafka Cluster and creating topics can
 * be found at 
 * {@link http://kafka.apache.org/documentation.html#quickstart}.
 * 
 * <p>For example:
 * <ul>
 * <li>download the Kafka code and untar it</li>
 * <li>{@code bin/zookeeper-server-start.sh config/zookeeper.properties &}</li>
 * <li>{@code bin/kafka-server-start.sh config/server.properties &}</li>
 * <li>{@code bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testTopic1}</li>
 * <li>{@code bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testTopic2}</li>
 * <li>{@code bin/kafka-topics.sh --list --zookeeper localhost:2181}</li>
 * </ul>
 */
package com.ibm.streamsx.topology.test.messaging.kafka;

