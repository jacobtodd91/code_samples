///////////////////////////////////////////////////////////////////////////
// Copyright © Esri. All Rights Reserved.
//
// Licensed under the Apache License Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
///////////////////////////////////////////////////////////////////////////
define(['dojo/_base/declare', 
'dojo/on', 
'dojo/dom', 
"dojo/dom-construct", 
"dijit/layout/TabContainer", 
"dijit/layout/ContentPane", 
'dijit/_WidgetBase',
'dijit/_TemplatedMixin',
"dojo/store/Memory", 
"dijit/form/ComboBox", 
"dojo/parser", 
"dojo/_base/array", 
"dojo/number", 
"dijit/registry", 
"esri/tasks/QueryTask", 
"esri/tasks/query", 
"esri/layers/FeatureLayer", 
'esri/symbols/SimpleLineSymbol',
"esri/Color",
"jimu/dijit/FeatureSetChooserForSingleLayer", 
"jimu/dijit/DrawBox", 
"dojo/domReady!"],
function(declare, on, dom, dojoConstruct, TabContainer, ContentPane, _WidgetBase, _TemplatedMixin, Memory, ComboBox, parser, array, number, registry, QueryTask, Query, esriFeatureLayer, Color, SimpleLineSymbol, jimu_featureset, jimu_drawbox) {
  //To create a widget, you need to derive from BaseWidget.
  return declare([_WidgetBase, _TemplatedMixin], {
    // DemoWidget code goes here

    //please note that this property is be set by the framework when widget is loaded.
    //templateString: template,

    baseClass: 'test',

    postCreate: function() {
      this.inherited(arguments);

      //Set select color
      var selectionColor = new Color([0,255,255])
      //Set selection symbology
      this.defaultLineSymbol = new SimpleLineSymbol(SimpleLineSymbol.STYLE_SOLID, selectionColor, 2);
      
      this_ = this;
      adoptACityStreets = map.getLayer("AdoptACityStreet_4375");
      availableStreets = adoptACityStreets['url'] + "/1";
      selectionEnabled = false;
      selectedStreets = [];

      console.log('postCreate');
    },
    makeSelection: function makeSelection(map){
        dojo.connect(map, "onClick", this.mapClicked)
    },
    mapClicked: function mapClicked(evt){
      queryTaskMapPoint = evt.mapPoint 
      console.log(queryTaskMapPoint)

      if(selectionEnabled){
        this_.selectFeatures(queryTaskMapPoint);
      }
    },
    selectFeatures: function(point){
      var queryPolygon = this_.createPolygonFromMapPoint(point,6,map) 
      this_.queryStreets(availableStreets, queryPolygon);
    },
    queryStreets: function(layer, queryGeometry){
      var query = new Query();
      var queryTask = new QueryTask(layer);
      query.returnGeometry = true;
      query.geometry = queryGeometry;
      query.outFields = ["WHOLESTNAME", "Shape.STLength()"]
      query.spatialRelationship = query.SPATIAL_REL_INTERSECTS;
      queryTask.execute(query, function(results){
        if(results){
          this_.populateStreetTable(results)
          fl.add(results)
        }
      })
    },
    createPolygonFromMapPoint: function createPolygonFromMapPoint(mapPoint,pixelTolerance,map){ 
      //create as sqare polygon based on scale of the map 
      //map.width= width in pixels, 
      //map.extent.getwidth() = width in mapUnits 
      //determine the offset in mapUnits 
      var offset = pixelTolerance * map.extent.getWidth() / map.width 
      console.log(offset);
      //Get the points of the square using the offset 
      var llPoint = mapPoint.offset(-offset,-offset); 
      var ulPoint = mapPoint.offset(-offset,offset); 
      var urPoint = mapPoint.offset(offset,offset); 
      var lrPoint = mapPoint.offset(offset,-offset); 
      //create a polygon in same spatialReference 
      var poly =  new esri.geometry.Polygon(mapPoint.spatialReference); 
      //Add the ring of points to the polygon.  don't cross lastpoint must = firstPoint 
      poly.addRing([llPoint, ulPoint, urPoint,lrPoint, llPoint]); 
      
      return poly; 
    },
    onOpen: function(){
      console.log('onOpen');
    },

    onClose: function(){
      console.log('onClose');
    },

    onMinimize: function(){
      console.log('onMinimize');
    },

    onMaximize: function(){
      console.log('onMaximize');
    },

    onSignIn: function(credential){
      /* jshint unused:false*/
      console.log('onSignIn');
    },

    onSignOut: function(){
      console.log('onSignOut');
    },

    showVertexCount: function(count){
      this.vertexCount.innerHTML = 'The vertex count is: ' + count;
    }
  });
});