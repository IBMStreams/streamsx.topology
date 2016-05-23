
from streamsx.topology import graph
from streamsx.topology.topology import *
from streamsx.topology import schema
from enum import Enum


class mqtt_streams(object):
 #
 # A simple connector to a MQTT broker for publishing
 # string tuples to MQTT topics, and
 # subscribing to MQTT topics and creating streams.
 # <p>
 # A connector is for a specific MQTT Broker as specified in
 # the configuration object config. Any number of  publish()and  subscribe()
 # connections may be created from a single mqtt_streams connector.
 # 
 # Sample use:
 #
 # topo = Topology("An MQTT application")
 # // optionally, define configuration information
 # config = {}
 # config['clientID'] = "test_MQTTpublishClient"
 # config['defaultQOS'] = 1  (needs to be int vs long)
 # config['qos'] = int("1") #(needs to be int vs long)
 # config['keepAliveInterval'] = int(20) (needs to be int vs long)
 # config['commandTimeoutMsec'] = 30000 (needs to be int vs long)
 # config['reconnectDelayMsec'] = 5000 (needs to be int vs long)
 # config['receiveBufferSize'] = 10 (needs to be int vs long)
 # config['reconnectionBound'] = int(20)
 # config['retain'] = True 
 # config['password'] = "foobar"
 # config['trustStore'] = "/tmp/no-such-trustStore"
 # config['trustStorePassword'] = "woohoo"
 # config['keyStore'] = "/tmp/no-such-keyStore"
 # config['keyStorePassword'] = "woohoo"

 # 
 # // create the connector's configuration property map
 # config['serverURI'] = "tcp://localhost:1883"
 # config['userID'] = "user1id"
 # config[' password'] = "user1passwrd"
 # 
 # // create the connector
 # mqstream = mqtt_streams(topo,config)
 # 
 # // publish a python source stream to the topic "python.topic1"
 # topic = "python.topic1"
 # src = topo.source(test_functions.mqtt_publish)
 # mqs = mqstream.publish(src, topic) 
 # streamsx.topology.context.submit("BUNDLE", topo.graph)
 # 
 # // subscribe to the topic "python.topic1"
 # topic = ["python.topic1", ]
 # mqs = mqstream.subscribe(topic) 
 # mqs.print()
 # 
 # Configuration properties apply to {@code publish} and
 # {@code subscribe} unless stated otherwise. 
 # <p>
 # <table border=1>
 # <tr><th>Property</th><th>Description</th></tr>
 # <tr><td>serverURI</td>
 #      <td>Required String. URI to the MQTT server, either
 #      {@code tcp://<hostid>[:<port>]}
 #      or {@code ssl://<hostid>[:<port>]}. 
 #      The port defaults to 1883 for "tcp:" and 8883 for "ssl:" URIs.
 #      </td></tr>
 # <tr><td>clientID</td>
 #      <td>Optional String. A unique identifier for a connection
 #      to the MQTT server. 
 #      The MQTT broker only allows a single
 #      connection for a particular {@code clientID}.
 #      By default a unique client ID is automatically
 #      generated for each use of {@code publish()} and {@code subscribe()}.
 #      The specified clientID is used for the first
 #      use {@code publish()} or {@code subscribe()} use and
 #      suffix is added for each subsequent uses.
 #      </td></tr>
 # <tr><td>keepAliveInterval</td>
 #      <td>Optional Integer.  Automatically generate a MQTT
 #      ping message to the server if a message or ping hasn't been
 #      sent or received in the last keelAliveInterval seconds.  
 #      Enables the client to detect if the server is no longer available
 #      without having to wait for the TCP/IP timeout.  
 #      A value of 0 disables keepalive processing.
 #      The default is 60.
 #      </td></tr>
 # <tr><td>commandTimeoutMsec</td>
 #      <td>Optional Long. The maximum time in milliseconds
 #      to wait for a MQTT connect or publish action to complete.
 #      A value of 0 causes the client to wait indefinitely.
 #      The default is 0.
 #      </td></tr>
 # <tr><td>reconnectDelayMsec</td>
 #      <td>Optional Long. The time in milliseconds before
 #      attempting to reconnect to the server following a connection failure.
 #      The default is 60000.
 #      </td></tr>
 # <tr><td>userID</td>
 #      <td>Optional String.  The identifier to use when authenticating
 #      with a server configured to require that form of authentication.
 #      </td></tr>
 # <tr><td>password</td>
 #      <td>Optional String.  The identifier to use when authenticating
 #      with server configured to require that form of authentication.
 #      </td></tr>
 # <tr><td>trustStore</td>
 #      <td>Optional String. The pathname to a file containing the
 #      public certificate of trusted MQTT servers.  If a relative path
 #      is specified, the path is relative to the application directory.
 #      Required when connecting to a MQTT server with an 
 #      ssl:/... serverURI.
 #      </td></tr>
 # <tr><td>trustStorePassword</td>
 #      <td>Required String when {@code trustStore} is used.
 #      The password needed to access the encrypted trustStore file.
 #      </td></tr>
 # <tr><td>keyStore</td>
 #      <td>Optional String. The pathname to a file containing the
 #      MQTT client's public private key certificates.
 #      If a relative path is specified, the path is relative to the
 #      application directory. 
 #      Required when an MQTT server is configured to use SSL client authentication.
 #      </td></tr>
 # <tr><td>keyStorePassword</td>
 #      <td>Required String when {@code keyStore} is used.
 #      The password needed to access the encrypted keyStore file.
 #      </td></tr>
 # <tr><td>receiveBufferSize</td>
 #      <td>[subscribe] Optional Integer. The size, in number
 #      of messages, of the subscriber's internal receive buffer.  Received
 #      messages are added to the buffer prior to being converted to a
 #      stream tuple. The receiver blocks when the buffer is full.
 #      The default is 50.
 #      </td></tr>
 # <tr><td>retain</td>
 #      <td>[publish] Optional Boolean. Indicates if messages should be
 #      retained on the MQTT server.  Default is false.
 #      </td></tr>
 # <tr><td>defaultQOS</td>
 #      <td>Optional Integer. The default
 #      MQTT quality of service used for message handling.
 #      The default is 0.
 #      </td></tr>
 # </table>
 # 
 # @see <a href="http://mqtt.org">http://mqtt.org</a>
 # @see <a
 #      href="http://ibmstreams.github.io/streamsx.messaging/">com.ibm.streamsx.messaging</a>


    def __init__(self,topology,config) :
        self.topology = topology
        self.config = config.copy()
        self.opCnt = 0
        
    def publish(self,src_op,topic=None) :
        if topic is None:
            raise TypeError("No topic provided to MqttStreams.publish")
        parms = self.config.copy()
        #parms['reconnectionBound'] = int(-1) (needs to be int vs long)
        #parms['qos'] = int(0) (needs to be int vs long)
        #del parms['messageQueueSize']
        if topic is None :
            parms['topicOutAttrName'] = "topic"
        else :
            parms['topic'] = topic
        parms['dataAttributeName'] = "message"
        if (++self.opCnt > 1) :
            # each op requires its own clientID
            clientId =  params['clientID']
            if (clientId is None and len(clientId) > 0) :
                params['clientID'] = opCnt+"-"+clientId
        # convert src_op outputport schema from spl po to spl rstring type
        forOp = self.topology.graph.addOperator("com.ibm.streamsx.topology.functional.python::PyFunctionFormat")
        forOp.addInputPort(outputPort=src_op.oport)
        forOport = forOp.addOutputPort(schema=schema.StreamSchema("tuple<rstring message>"))                
        op = self.topology.graph.addOperator(kind="com.ibm.streamsx.messaging.mqtt::MQTTSink")
        oport = op.addOutputPort(schema=schema.StreamSchema("tuple<rstring message>"))
        op.addInputPort(outputPort=forOport)
        op.setParameters(parms)
        return None
    
    def subscribe(self,topic=None) :
        if topic is None:
            raise TypeError("No topic provided to MqttStreasm.subscribe")
        parms = self.config.copy()
        #parms['reconnectionBound'] = int(-1) (needs to be int vs long)
        #parms['qos'] = int(0) (needs to be int vs long)
        del parms['retain']
        parms['topics'] = topic
        parms['topicOutAttrName'] = "topic"
        #parms['dataAttributeName'] = "message"
        parms['dataAttributeName'] = "message"
        if (++self.opCnt > 1) :
            # each op requires its own clientID
            clientId =  params['clientID']
            if (clientId is None and len(clientId) > 0) :
                params['clientID'] = opCnt+"-"+clientId
        op = self.topology.graph.addOperator(kind="com.ibm.streamsx.messaging.mqtt::MQTTSource")
        oport = op.addOutputPort(schema=schema.StreamSchema("tuple<rstring topic, rstring message>"))
        op.setParameters(parms)
        pop = self.topology.graph.addPassThruOperator()
        pop.addInputPort(outputPort=oport)
        pOport = pop.addOutputPort(schema=schema.StreamSchema("tuple<rstring message>"))
        return Stream(self.topology, pOport)
            