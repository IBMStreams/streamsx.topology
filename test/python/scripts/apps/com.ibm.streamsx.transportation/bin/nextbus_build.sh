rm -fr sabs
mkdir sabs
export STREAMS_SPLPATH=$HOME/toolkits:$STREAMS_INSTALL/toolkits

for mc in \
    com.ibm.streamsx.transportation.nextbus.services::AgencyLocationsService \
    com.ibm.streamsx.transportation.nextbus.services::MapBusLocations \
    com.ibm.streamsx.transportation.mapping.services::MappingService 
do
  rm -fr output
  sc --no-toolkit-indexing --main-composite $mc
  mv output/*.sab sabs/
  rm -fr output
done
