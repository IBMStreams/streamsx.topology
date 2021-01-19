# Changes
==========

## v2.1.0
* [#2649](https://github.com/IBMStreams/streamsx.topology/issues/2649) Python scripts: Print diagnostic info in verbose mode when spl-python-extract skips python modules
* [#2646](https://github.com/IBMStreams/streamsx.topology/issues/2646) Python: Support new consumingReads parameter of spl.endpoint::EndpointSink
* [#2640](https://github.com/IBMStreams/streamsx.topology/issues/2640) Python: Support SPL annotation @catch
* [#2590](https://github.com/IBMStreams/streamsx.topology/issues/2590) Python: Support definition of event-time streams
* [#2589](https://github.com/IBMStreams/streamsx.topology/issues/2589) Python: Support definition of TimeInterval windows
* [#2639](https://github.com/IBMStreams/streamsx.topology/issues/2639) Documentation: Typo in ConsistentRegionConfig example

## v2.0.0
* [#2627](https://github.com/IBMStreams/streamsx.topology/issues/2627) `streamsx-streamtool lstoolkits` should show the toolkit-ID
* [#2609](https://github.com/IBMStreams/streamsx.topology/issues/2609) CPD3.5: "invalid platform token" error

## v1.18.0
* [#2610](https://github.com/IBMStreams/streamsx.topology/issues/2610) Support Streams job as CP4D service: New classes EndpointSink, EndpointSource in streamsx.service
* [#2607](https://github.com/IBMStreams/streamsx.topology/issues/2607) Documentation updates in Python API to resolve wrong or missing descriptions

## v1.17.1
* [#2605](https://github.com/IBMStreams/streamsx.topology/issues/2605) Resolve submission issue when PROJECT_ID is missing inside CP4D cluster

## v1.17.0
* [#2598](https://github.com/IBMStreams/streamsx.topology/issues/2598) Third-party libraries updated to resolve potential security vulnerabilities (httpclient 4.5.13)
* [#2596](https://github.com/IBMStreams/streamsx.topology/issues/2596) Fix broken JSON4J link in Java documentation
* [#2594](https://github.com/IBMStreams/streamsx.topology/issues/2594) Support of the new CP4D Jobs REST API
* [#2587](https://github.com/IBMStreams/streamsx.topology/issues/2587) Fix conda detection in Python 3.7

## v1.16.0
* [#1679](https://github.com/IBMStreams/streamsx.topology/issues/1679) Nested tuple: Support tuple as attribute type
* [#2551](https://github.com/IBMStreams/streamsx.topology/issues/2551) Adapt streamsx for REST API changes
* [#2527](https://github.com/IBMStreams/streamsx.topology/issues/2527) Python: Support tumbling window based on punctuation
* [#2525](https://github.com/IBMStreams/streamsx.topology/issues/2525) Python: Provide punct_count() in TopologyTester to test the number of received punctuations
* [#2520](https://github.com/IBMStreams/streamsx.topology/issues/2520) Python: Ability to handle received punctuations in for_each and streamsx.spl.spl.PrimitiveOperator
* [#2518](https://github.com/IBMStreams/streamsx.topology/issues/2518) Python: Ability to submit punctuation in streamsx.spl.spl.PrimitiveOperator
* [#2517](https://github.com/IBMStreams/streamsx.topology/issues/2517) Python: Provide Stream.punctor()
* [#2516](https://github.com/IBMStreams/streamsx.topology/issues/2516) Python: Write punctuation markers with Stream.print() to stdout

## v1.15.10
* [#2561](https://github.com/IBMStreams/streamsx.topology/issues/2561) Third-party lib updated to resolve security vulnerabilities (commons-compress: 1.19)

## v1.15.9
* [#2522](https://github.com/IBMStreams/streamsx.topology/issues/2522) Build scripts prepared for publishing the release to Maven repository
* [#2537](https://github.com/IBMStreams/streamsx.topology/issues/2537) TestTopology: Check OS version to skip tests in SPLOperatorsTest.java for RH6
* [#2533](https://github.com/IBMStreams/streamsx.topology/issues/2533) Automatic creation of edge image name can result in illegal image name

## v1.15.8
* [#2529](https://github.com/IBMStreams/streamsx.topology/issues/2529) Provide failure reason if edge image build fails
* [#2524](https://github.com/IBMStreams/streamsx.topology/issues/2524) JAVA: Resolve issue in keyed window definition and using submission parameter

## v1.15.7
* [#2513](https://github.com/IBMStreams/streamsx.topology/issues/2513) Third-party lib updated to resolve security vulnerabilities (commons-codec: 1.14)

## v1.15.6
* [#2508](https://github.com/IBMStreams/streamsx.topology/issues/2508) Python: Support tumbling window creation using submission params
* [#2506](https://github.com/IBMStreams/streamsx.topology/issues/2506) Python: Window.aggregate supports submission parameter for size of window eviction policy

## v1.15.5
* [#2499](https://github.com/IBMStreams/streamsx.topology/issues/2499) Pythondoc: Updated `Stream.parallel`,`Stream.set_parallel` args description regarding submission parameter
* [#2498](https://github.com/IBMStreams/streamsx.topology/issues/2498) Python: Support sliding window creation using submission parameter
* [#2494](https://github.com/IBMStreams/streamsx.topology/issues/2494) JAVA: Support sliding window creation using submission parameter
* [#2495](https://github.com/IBMStreams/streamsx.topology/issues/2495) edgeConfig: `condaPackages` parameter is not supported anymore

## v1.15.4
* [#2488](https://github.com/IBMStreams/streamsx.topology/issues/2488) execution context: Make submission time parameters available without serializing the parameter callable

## v1.15.3
* [#2490](https://github.com/IBMStreams/streamsx.topology/issues/2490) Resolve issue with streamsx-streamtool caused by changes in v1.15.1

## v1.15.2
* [#2486](https://github.com/IBMStreams/streamsx.topology/issues/2486) Support type hints with Python `flat_map`

## v1.15.1
* [#2484](https://github.com/IBMStreams/streamsx.topology/issues/2484) Document known issues in Python doc
* [#2478](https://github.com/IBMStreams/streamsx.topology/issues/2478) Support additional fields like `pipPackages` and `rpms` be provisioned for the "edge" image creation
* [#2476](https://github.com/IBMStreams/streamsx.topology/issues/2476) Python: `import streamsx.rest_primitives` raises `ImportError`
* [#2474](https://github.com/IBMStreams/streamsx.topology/issues/2474) Fix topology name and namespace when one of them is an SPL reserved word
* [#1666](https://github.com/IBMStreams/streamsx.topology/issues/1666) Prevent sc error caused by end_parallel on non-parallel stream

## v1.15.0:
* [#2466](https://github.com/IBMStreams/streamsx.topology/issues/2466) Javadoc: Wrong version information resolved
* [#2447](https://github.com/IBMStreams/streamsx.topology/issues/2447) Support invocation to build an application for the edge
* [#2429](https://github.com/IBMStreams/streamsx.topology/issues/2429) Out of memory error when using large python packages
* [#2427](https://github.com/IBMStreams/streamsx.topology/issues/2427) streamsx can be used with Java 8 to 11.
* [#1478](https://github.com/IBMStreams/streamsx.topology/issues/1478) Python: Stream.filter() supports additional output stream for non-matching tuples
* [#1469](https://github.com/IBMStreams/streamsx.topology/issues/1469) Python: Support parallel partitioning by attribute for structured streams

## v1.14.15:
* [#2423](https://github.com/IBMStreams/streamsx.topology/issues/2423) REST/Java: connect timeout of 30s added

## v1.14.14:
* [#2416](https://github.com/IBMStreams/streamsx.topology/issues/2416) Python aggregate does support dict or named tuple output
* [#2414](https://github.com/IBMStreams/streamsx.topology/issues/2414) SPL operator invocation: Issue with setting output assignment for reserved names/properties
* [#2404](https://github.com/IBMStreams/streamsx.topology/issues/2404) Documentation: Add timeout to queue.get
* [#2401](https://github.com/IBMStreams/streamsx.topology/issues/2401) Add a timeout to the REST requests
* [#2396](https://github.com/IBMStreams/streamsx.topology/issues/2396) REST query failures should give meaningful error messages
* [#756](https://github.com/IBMStreams/streamsx.topology/issues/756) Symbolic link to python modules not copied into build archive

## v1.14.13:
* [#2392](https://github.com/IBMStreams/streamsx.topology/issues/2392) Error when calling streamsx-streamtool without sub command
* [#2314](https://github.com/IBMStreams/streamsx.topology/issues/2314) add_pip_package supports URL to package or wheel file
* [#2285](https://github.com/IBMStreams/streamsx.topology/issues/2285) streamsx-sc and streamsx-runner support main composites without namespace
* [#994](https://github.com/IBMStreams/streamsx.topology/issues/994) Documentation updated for Python module (streamsx.topology.context.submit|SubmissionResult)
* [#942](https://github.com/IBMStreams/streamsx.topology/issues/942) code clean-up

## v1.14.11:
* [#1396](https://github.com/IBMStreams/streamsx.topology/issues/1396) Documentation: --enabled-shared misspelled
* [#2360](https://github.com/IBMStreams/streamsx.topology/issues/2360) Cannot submit to public cloud service using credentials only
* [#2381](https://github.com/IBMStreams/streamsx.topology/issues/2381) Remote build fails when submitted from Windows

## v1.14.10:
* [#2358](https://github.com/IBMStreams/streamsx.topology/issues/2358) PE crash with Python Source operator having dict or named tuple output style
* [#258](https://github.com/IBMStreams/streamsx.topology/issues/258) Resolve Scala sample build issue
* [#250](https://github.com/IBMStreams/streamsx.topology/issues/250) Clean-up enhanced in build script

## v1.14.9:
* [#2353](https://github.com/IBMStreams/streamsx.topology/issues/2353) add_toolkit_dependency fails when the version contains space

## Older releases
Please consult the release notes for the release you are interested in.
