# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2019
import unittest
import os
import test_functions

from streamsx.topology.topology import *
from streamsx.topology import schema
import streamsx.topology.context
from streamsx.topology.mqtt import *

class TestTopologyMethods(unittest.TestCase):

# These tests rely on a MQTT server running on localhost port 1883
# I used the mosquitto MQTT server available here: http://mosquitto.org/download/
# follow install instructions then start the server to listen on port 1883 via the cmd "/usr/sbin/mosquitto -p 18813 -v"
# There are also problems with the standalone environment such that the publish test disconnect before publishing data.  Solution TBD
   
  def test_MQTTpublish(self):
    topo = Topology("test_TopologyMQTTpublish")
    config = {} 
    #config['defaultQOS'] = 1  (needs to be int vs long)
    #config['qos'] = int("1") #(needs to be int vs long)
    #config['keepAliveInterval'] = int(20) (needs to be int vs long)
    #config['commandTimeoutMsec'] = 30000 (needs to be int vs long)
    #config['reconnectDelayMsec'] = 5000 (needs to be int vs long)
    #config['receiveBufferSize'] = 10 (needs to be int vs long)
    config['clientID'] = "test_MQTTpublishClient"
    config['serverURI'] = "tcp://localhost:1883"
    #config['reconnectionBound'] = int(20)
    config['retain'] = True
    config['userID'] = "user1id"  
    config['password'] = "foobar"
    config['trustStore'] = "/tmp/no-such-trustStore"
    config['trustStorePassword'] = "woohoo"
    config['keyStore'] = "/tmp/no-such-keyStore"
    config['keyStorePassword'] = "woohoo"
    mqstream = MqttStreams(topo,config)
    topic = "python.topic1"
    src = topo.source(test_functions.mqtt_publish)
    mqstream.publish(src, topic) 
    streamsx.topology.context.submit("BUNDLE", topo.graph)     

      
  def test_MQTTsubscribe(self):
    topo = Topology("test_TopologyMQTTsubscribe")
    config = {} 
    #config['defaultQOS'] = 1  (needs to be int vs long)
    #config['qos'] = int(1) (needs to be int vs long)
    #config['keepAliveInterval'] = int(20) (needs to be int vs long)
    #config['commandTimeoutMsec'] = 30000 (needs to be int vs long)
    #config['reconnectDelayMsec'] = 5000 (needs to be int vs long)
    #config['receiveBufferSize'] = 10 (needs to be int vs long)
    config['clientID'] = "test_MQTTsubscribeClient"
    config['serverURI'] = "tcp://localhost:1883"
    #config['reconnectionBound'] = int(20)
    config['retain'] = False
    config['userID'] = "user"  
    config['password'] = "foobar"
    config['trustStore'] = "/tmp/no-such-trustStore"
    config['trustStorePassword'] = "woohoo"
    config['keyStore'] = "/tmp/no-such-keyStore"
    config['keyStorePassword'] = "woohoo"
    mqstream = MqttStreams(topo,config)
    topic = ["python.topic1", "python.topic2", ]
    mqs = mqstream.subscribe(topic) 
    mqs.print()
    mqs.for_each(test_functions.mqtt_subscribe)
    streamsx.topology.context.submit("BUNDLE", topo.graph)

  def test_MQTTpublishClass(self):
    topo = Topology("test_TopologyMQTTpublishClass")
    config = {} 
    #config['qos'] = int("1") #(needs to be int vs long)
    #config['keepAliveInterval'] = int(20) (needs to be int vs long)
    #config['commandTimeout'] = 30000 (needs to be int vs long)
    #config['period'] = 5000 (needs to be int vs long)
    #config['messageQueueSize'] = 10 (needs to be int vs long)
    config['clientID'] = "test_MQTTpublishClassClient"
    config['serverURI'] = "tcp://localhost:1883"
    #config['reconnectionBound'] = int(20)
    config['retain'] = True
    config['userID'] = "user1id"  
    config['password'] = "foobar"
    config['trustStore'] = "/tmp/no-such-trustStore"
    config['trustStorePassword'] = "woohoo"
    config['keyStore'] = "/tmp/no-such-keyStore"
    config['keyStorePassword'] = "woohoo"
    mqstream = MqttStreams(topo,config)
    topic = "python.class.topic1"
    src = topo.source(test_functions.mqtt_publish_class)
    mqstream.publish(src, topic) 
    streamsx.topology.context.submit("BUNDLE", topo.graph)     

      
  def test_MQTTsubscribeClass(self):
    topo = Topology("test_TopologyMQTTsubscribeClass")
    config = {} 
    #config['defaultQOS'] = 1  (needs to be int vs long)
    #config['qos'] = int(1) (needs to be int vs long)
    #config['keepAliveInterval'] = int(20) (needs to be int vs long)
    #config['commandTimeoutMsec'] = 30000 (needs to be int vs long)
    #config['reconnectDelayMsec'] = 5000 (needs to be int vs long)
    #config['receiveBufferSize'] = 10 (needs to be int vs long)
    config['clientID'] = "test_MQTTsubscribeClassClient"
    config['serverURI'] = "tcp://localhost:1883"
    #config['reconnectionBound'] = int(20)
    config['retain'] = False
    config['userID'] = "user"  
    config['password'] = "foobar"
    config['trustStore'] = "/tmp/no-such-trustStore"
    config['trustStorePassword'] = "woohoo"
    config['keyStore'] = "/tmp/no-such-keyStore"
    config['keyStorePassword'] = "woohoo"
    mqstream = MqttStreams(topo,config)
    topic = ["python.class.topic1", ]
    mqs = mqstream.subscribe(topic) 
    mqs.print()
    mqs.for_each(test_functions.mqtt_subscribe_class)
    streamsx.topology.context.submit("BUNDLE", topo.graph)

          
if __name__ == '__main__':
    unittest.main()



