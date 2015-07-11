# streamsx.topology
A project that supports building streaming topologies (applications)
for IBM Streams in different programming languages, such as Java and Scala.
http://ibmstreams.github.io/streamsx.topology/

## Java Application API
The Java Application API enables a developer to create streaming applications entirely in Java for IBM Streams. The API employs a functional style of programming -- a developer may define a graph's flow and data manipulation simultaneously.

Please refer to the [getting started guide](http://ibmstreams.github.io/streamsx.topology/gettingstarted.html), [FAQ page](http://ibmstreams.github.io/streamsx.topology/FAQ.html), and [documentation](http://ibmstreams.github.io/streamsx.topology/doc.html) for help.

## Scala Application API
The Scala Application API enables a developer to create streaming applications entirely in Scala for IBM Streams. The Scala API is currently calls into the Java Application API (as Java & Scala are both JVM languages), and includes implicit conversions to allow Scala anonymous functions to be used as the functional transformations.

Please see this initial documentation: https://github.com/IBMStreams/streamsx.topology/wiki/Scala-Support
