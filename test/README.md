;; begin_generated_IBM_copyright_prolog                             
;;                                                                  
;; This is an automatically generated copyright prolog.             
;; After initializing,  DO NOT MODIFY OR MOVE                       
;; **************************************************************** 
;; Licensed Materials - Property of IBM                             
;; 5724-Y95                                                         
;; (C) Copyright IBM Corp.  2016, 2016    All Rights Reserved.      
;; US Government Users Restricted Rights - Use, duplication or      
;; disclosure restricted by GSA ADP Schedule Contract with          
;; IBM Corp.                                                        
;;                                                                  
;; end_generated_IBM_copyright_prolog                               
# Testing

## Full Tests

To run the full set of tests

```
cd streamsx.topology
ant test

# Run distributed tests
# Requires a Streams instance is running and
# streamtool submitjob does not require 
# authentication input.
cd test/java
ant unittest.distributed
```

## Changing Streams install or toolkit release

By default the tests run against:
 * Streams release at $STREAMS_INSTALL
   * including compiling the underlying SPL applications
   * running or submitting the bundles
 * Toolkit at `streamsx.topology/com.ibm.streamsx.topology`

The version of Streams used to compile the applications can be
set by using the `topology.install.compile` ant property.

```
cd streamsx.topology
ant -Dtopology.install.compile=/opt/ibm/InfoSphere_Streams/4.0.1.0 test
```
Note the applications are still submitted to the default running instance,
so this can test that applications built against 4.0.1 continue to work
against a newer release.

The release of the toolkit used can be specified using `topology.toolkit.release`.

```
cd streamsx.topology
ant clean
ant -Dtopology.toolkit.release=$HOME/testtk/com.ibm.streamsx.topology test
```

The `clean` ensures that the tests are not picking up the toolkit in the code.

This is useful for testing the release process produces a workable toolkit and that a released version runs against a different version of Streams. E.g. build using Streams 4.1, test on 4.0.1.
