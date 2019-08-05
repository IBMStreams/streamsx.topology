getQueryParameters = function() {
        return dojo.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search[0] === "?" ? 1 : 0)));
}

/**

 Periodically fetch JSON data from a URL (typically from
 HTTPTupleView) and call the passed in update function
 with the json response.
  
*/
periodicUpdateJSON = function(url, period, update) {
      getData = function(url, update) {
         dojo.xhrGet({ url: url, handleAs: "json", load: update});
     };

      getData(url, update);
      return setInterval(function() {getData(url, update);}, period * 1000);
};

/*
 Periodically fetch JSON data from a URL (typically from
 HTTPTupleView) and call the passed in update function
 with a dojo.store.Memory instance containing the data.

 url - url to fetch
 period - update period in seconds
 update - function to call after data fetch
*/
periodicUpdateMemory = function(url, period, update) {

  var intervalId;
  require(["dojo/store/Memory"],
   function(Memory) {
      
      intervalId = periodicUpdateJSON(url, period, function(response) {
          var store = new Memory({ data: [] });
          store.setData(response);
          update(store);
      });
   });
   return intervalId;
};

gridUpdate = function(grid, store) {
   require(["dojo/data/ObjectStore"],
   function(ObjectStore) {
     grid.setStore(new ObjectStore({ objectStore: store }));
   });
};

periodicUpdateGrid = function(url, period, grid) {
   return periodicUpdateMemory(url, period, function(store) { gridUpdate(grid, store); });
};

var emptyStore;

require(["dojo/store/Memory"], function(Memory) {
    emptyStore = new Memory({ data: [] });
});
var emptyDataStore;
require(["dojo/data/ObjectStore"], function(ObjectStore) {
    emptyDataStore = new ObjectStore({ objectStore: emptyStore });
});
