## Nextbus

### Running with QSE

1. Download releases of:
    * `com.ibm.streamsx.inet`  - https://github.com/IBMStreams/streamsx.inetserver/releases/tag/v3.0.0 (or a later 3.x release)
    * `com.ibm.streamsx.inetserver` - https://github.com/IBMStreams/streamsx.inet/releases/tag/v3.0.0 (or a later 3.x release)
    * `com.ibm.streamsx.transportation` - https://github.com/IBMStreams/streamsx.transportation/releases/tag/v2.0.0.alpha
1. Unpack archives into `$HOME/toolkits`
1. In an empty directory (not under $HOME/toolkits) execute to produce three sab files under the subdirectory `sabs`
    * `$HOME/toolkits/com.ibm.streamsx.transportation/bin/nextbus_build.sh`
1. Submit each of the `sabs` to your QSE instance, the AgencyLocations will default to `sf-muni`.

1. Open the browser to http://localhost:8080/streamsx.inet.resources/dojo/viewall.html
1. Click on the live map link - all of the buses in San Francisco should be shown. Clicking on a bus shows a pop-up with additional info.

Additional agencies can be added by submitting the AgencyLocationsService sab with a different agency.
The list of Nextbus agencies is here: http://webservices.nextbus.com/service/publicXMLFeed?command=agencyList

![image](https://user-images.githubusercontent.com/3769612/50355418-7b1c3e00-0503-11e9-8723-56bcab9e9965.png)
