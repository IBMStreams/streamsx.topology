
JSONreplacer = function(key, value)
{
   if (value == "" || key == "markerType")
      return undefined;
   
   return value;
}

// If the tuple contains a note attribute
// then use that as the popup text.
//
// otherwise produce a "pretty" version
// of the tuple using JSON.
makePopupText = function(tuple) {
   if (tuple.note != undefined)
       return tuple.note;
   
   return JSON.stringify(tuple, JSONreplacer, "<br />")
               .replace(/["{}]/g, "")   // get rid of JSON delimiters
               .substring(7);           // skip the first newline
}

createPopup = function(feature) {
   feature.popup = new OpenLayers.Popup.FramedCloud("Popup",
                           feature.geometry.getBounds().getCenterLonLat(),
                           null,
                           '<div>' + makePopupText(feature.attributes.spltuple) + '</div>',
                           null,
                           false,
                           function() { controls['selector'].unselectAll(); }
                        );
   feature.layer.map.addPopup(feature.popup);
}

destroyPopup = function(feature) {
   feature.popup.destroy();
   feature.popup = null;
}

createMarkerLayer = function(map, name) {
    var markerLayer = new OpenLayers.Layer.Vector(name);
    map.addLayer(markerLayer);
 
    //Add a selector control to the vectorLayer with popup functions
    var controls = {
         selector: new OpenLayers.Control.SelectFeature(markerLayer, { onSelect: createPopup, onUnselect: destroyPopup })
    };
      
    map.addControl(controls['selector']);
    controls['selector'].activate();
    
    return markerLayer;
}

moveMarker = function(feature, targetLoc) {
   feature.move(targetLoc);
   
   if (feature.popup) {
      feature.popup.setContentHTML('<div>' + makePopupText(feature.attributes.spltuple) + '</div>');
      feature.popup.updateSize();
      feature.popup.lonlat.lon = targetLoc.lon;
      feature.popup.lonlat.lat = targetLoc.lat;
      feature.popup.updatePosition();
   }
}

getMarkerLayer = function(markerLayers, defaultLayer, layer) {
   if (layer == undefined)
      return defaultLayer;

   var markerLayer = markerLayers[layer];
   if (markerLayer == undefined) {
        markerLayer = createMarkerLayer(defaultLayer.map, layer);
        markerLayers[layer] = markerLayer;
   }
   return markerLayer;
      
}

addMarkersToLayer = function(markerLayers, markers, response) {

   var defaultMarkerLayer = markerLayers["Markers"];
   var map = defaultMarkerLayer.map;

   var tuples = response;
   
   var epsg4326 = new OpenLayers.Projection("EPSG:4326");   
   var mapProjection = map.getProjectionObject();
      		
   var updated = [] ;
   			
   for (var i = 0; i < tuples.length; i++) {
      var tuple = tuples[i];
      var markerType = getMarkerGraphic(tuple.markerType);

      var markerLayer = getMarkerLayer(markerLayers, defaultMarkerLayer, tuple.layer);
      var id = markerLayer.name + ":" + tuple.id;
      updated.push(id);

      if (id in markers) {
         if (markers[id].attributes.spltuple.markerType != markerType)
            markers[id].style.externalGraphic = markerType;
         
         markers[id].attributes.spltuple = tuple;
         var point  = new OpenLayers.LonLat(tuple.longitude, tuple.latitude);
         point.transform(epsg4326, mapProjection);
         moveMarker(markers[id], point);
      } else {
         var point = new OpenLayers.Geometry.Point(tuple.longitude, tuple.latitude);
         point.transform(epsg4326, mapProjection);	
         var marker = new OpenLayers.Feature.Vector(point,
                              {spltuple: tuple},
                              {externalGraphic: markerType, 
                               graphicHeight: 25, graphicWidth: 21 /*,
                               graphicXOffset:-12, graphicYOffset:-25 */ 
                              }
                           );
         marker.fid = id;
         markerLayer.addFeatures(marker);
         marker.__splLayer = markerLayer;
         markers[id] = marker;
      
         // First time only: set map viewport if not already set for geofences
         if (i == 0 && map.getCenter() == undefined) {
            map.setCenter(
                     new OpenLayers.LonLat(tuple.longitude, tuple.latitude).transform(epsg4326, mapProjection),
                     12);
         }
      }
   }
   
   // Remove any markers for which there was no new value
   for (var id in markers) {
      if (markers.hasOwnProperty(id) && updated.indexOf(id) == -1) {
         var markerLayer = marker.__splLayer;
         marker.__splLayer == undefined;
         markerLayer.removeMarker(markers[id]);
         markers[id].spltuple = null;
         markers[id].style.icon = null;
         delete markers[id];
      }
   }
}

getMarkerGraphic = function( markerType) {
    if (markerType == undefined)
        return 'marker-blue.png';

    switch (markerType) {
     case 'GREEN':
          return 'marker-green.png';
     case 'YELLOW':
          return 'marker-gold.png';
     case 'RED':
          return 'marker-red.png';
     case 'BLUE':
          return 'marker-blue.png';
     case 'WARNING':
          return 'marker-warning.png';
     case 'AWARD':
          return 'marker-award.png';
     default:
         return markerType;
    }
}
