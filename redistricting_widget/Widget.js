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
'dijit/_WidgetsInTemplateMixin',
'jimu/BaseWidget', 
'dojo/on', 
'dojo/dom', 
"dojo/dom-construct", 
"dojo/dom-style",
"dojo/_base/array", 
"dojo/number", 
"dojo/_base/lang", 
"dojo/parser",
'dojo/query',
"esri/tasks/IdentifyTask",
"esri/tasks/IdentifyParameters",
"esri/tasks/QueryTask", 
"esri/tasks/query", 
"esri/geometry/normalizeUtils",
"esri/tasks/GeometryService",
"esri/tasks/BufferParameters",
"esri/tasks/Geoprocessor",
"esri/tasks/DistanceParameters",
"esri/layers/FeatureLayer", 
"esri/layers/GraphicsLayer",
"esri/symbols/SimpleMarkerSymbol",
'esri/symbols/SimpleLineSymbol',
"esri/symbols/SimpleFillSymbol",
"esri/symbols/TextSymbol",
"esri/symbols/Font",
"esri/Color",
'esri/request',
"esri/geometry/webMercatorUtils",  
"esri/SpatialReference", 
'esri/graphic',
"esri/graphicsUtils",
"esri/geometry/Point",
"esri/geometry/projection",
"esri/geometry/coordinateFormatter",
"esri/geometry/Polygon",
"esri/geometry/Circle",
"esri/dijit/PopupTemplate",
"esri/dijit/analysis/FindNearest",
'jimu/dijit/FeatureSetChooserForMultipleLayers', 
"dijit/registry",
"dijit/form/TextBox",
"dijit/form/Select",
"dojo/domReady!"],
function(declare, _WidgetsInTemplateMixin, BaseWidget, on, dom, dojoConstruct, domStyle, array, number, dojo_lang, parser, dojoQuery, esriidentifyTask, esriIdentifyParameters, QueryTask, Query, esriNormalizeUtils, esriGeometryService, esriBufferParameters, esriGeoprocessor, esriDistanceParameters, esriFeatureLayer, esriGraphicsLayer, esriSimpleMarkerSymbol, esriSimpleLineSymbol, esriSimpleFillSymbol, esriTextSymbol, esriFont, Color, esriRequest, esriWMUtils, esriSpatialReference, esriGraphic, esriGraphicsUtils, esriPoint, esriProjection, esriCoordinateFormatter, esriPolygon, esriCircle, esriPopupTemplate, esriFindNearest, FeatureSetChooserForMultipleLayers, registry, dijitTextBox, dijitSelect) {
  //To create a widget, you need to derive from BaseWidget.

  //The DrawBox.js clear function has been modified
  return declare([BaseWidget], {
    // DemoWidget code goes here

    //please note that this property is be set by the framework when widget is loaded.
    //templateString: template,

    baseClass: 'jimu-widget-demo',

    totalPopulation: 0,
    districtPopulation: 0,
    districtVoterPopulation: 0,
    precinctsArray: [],
    selectedPrecinctPopulation: 0,
    districtSelectionEnabled: true,
    selectedDistrict: null,
    mapClicked: null,
    selectionHandler: null,
    insideSymbol: null,
    insideHighlightSymbol: null,
    mapLayers: [],
    outsideSymbol: null,
    outsideHighlightSymbol: null,
    textSymbolFont: null,
    districtPopulationArray: [{"district": 1, "population": 114285},{"district": 2, "population": 133399},{"district": 3, "population": 142218},{"district": 4, "population": 133819},{"district": 5, "population": 116618},{"district": 6, "population": 113719},{"district": 7, "population": 125130},],
    allDistrictsPopulation: 879188,
    districtAveragePopulation: 125598,

    postCreate: function() {
      this.inherited(arguments);   
      
      this_ = this;
      map = this.map;

      //set selection symbology
      this.insideHighlightSymbol = new esriSimpleFillSymbol(esriSimpleFillSymbol.STYLE_SOLID,
        new esriSimpleLineSymbol(
          esriSimpleLineSymbol.STYLE_SOLID,
          new Color([0,0,0]), 2
        ),
        new Color([129,112,50])
      )
      this.outsideHighlightSymbol = new esriSimpleFillSymbol(esriSimpleFillSymbol.STYLE_SOLID,
        new esriSimpleLineSymbol(
          esriSimpleLineSymbol.STYLE_SOLID,
          new Color([0,0,0]), 2
        ),
        new Color([0,80,53])
      )
      this.insideSymbol = new esriSimpleFillSymbol(esriSimpleFillSymbol.STYLE_SOLID,
        new esriSimpleLineSymbol(
          esriSimpleLineSymbol.STYLE_SOLID,
          new Color([129,112,50]), 2
        ),
        new Color([129,112,50,0.25])
      )
      this.outsideSymbol = new esriSimpleFillSymbol(esriSimpleFillSymbol.STYLE_SOLID,
        new esriSimpleLineSymbol(
          esriSimpleLineSymbol.STYLE_SOLID,
          new Color([0,80,53]), 2
        ),
        new Color([0,80,53,0.25])
      )

      this.textSymbolFont = new esriFont("14pt",
      esriFont.STYLE_NORMAL,
      esriFont.VARIANT_NORMAL,
      esriFont.WEIGHT_BOLD, "Arial");

      console.log('postCreate');
    },
    startup: function() {
      this_ = this
      this.inherited(arguments);
      parser.parse();

      this.clearSelectionBtn.innerHTML = 'Clear Selection';

      //Create graphic layers and add to the map
      districtsSelectionLayer = new esriGraphicsLayer({id: 'districtsSelectionLayer'});
      precinctSelectionLayer = new esriGraphicsLayer({id: 'precinctSelectionLayer'});
      precinctsLayer = new esriGraphicsLayer({id: 'precincts'})

      map.addLayers([districtsSelectionLayer, precinctsLayer, precinctSelectionLayer])

      //Get all map layers
      this_.getMapLayers()

      map.on("click", this_.delegateSelection)

      //Activate the clear selection function
      on(this.clearSelectionBtn, "click", function(){
        console.log("District selection cleared")
        this_.totalPopulation = 0
        this_.districtPopulation = 0
        this_.selectedPrecinctPopulation = 0
      
        districtsSelectionLayer.clear()
        precinctSelectionLayer.clear()
        precinctsLayer.clear()

        this_.selectedDistrictElem.innerHTML = `0`
        this_.selectedDistrictPopElem.innerHTML = `0`
        this_.totalPopulationElem.innerHTML = `0`
        this_.selectedDistrictVarianceElem.innerHTML = `0`
        this_.selectedDistrictVarianceElem.innerHTML = `0`
        this_.totalPopulationVarianceElem.innerHTML = "0"
        this_.districtSelectionEnabled = true
      })
    },
    getMapLayers: function(mapLayers){
      mapLayers = this_.map.graphicsLayerIds
      for(let i = 0; i < mapLayers.length; i++){
        layer = this_.map.getLayer(mapLayers[i])
        this_.mapLayers.push(layer)
      }
    },
    delegateSelection: function(evt){
      if(evt.graphic._layer.id == this_.mapLayers[0]['id']){
        this_.districtSelectionEnabled = false

        if(this_.districtSelectionEnabled == false){
          console.log("A district has been selected")
          //Display district relation values in HTML
          this_.selectedDistrict = evt.graphic.attributes['district']          
          this_.selectedDistrictElem.innerHTML = this_.selectedDistrict
  
          this_.queryDistrictPrecincts() 
        }
      }
      else if(evt.graphic._layer.id == this_.mapLayers[4]['id']){
        this_.highlightSelection(this_.mapLayers[5],evt,this_.insideHighlightSymbol, this_.outsideHighlightSymbol)
        this_.getTotalPopulation(evt, "isAdded")
      }
      else if(evt.graphic._layer.id == this_.mapLayers[5]['id']){
        this_.removeSelection(this_.mapLayers[5],evt)
        console.log("selection removed")
        this_.getTotalPopulation(evt, "isRemoved")
      }
    },
    removeSelection: function(layer, evt){
      console.log("Unselected precinct: " + evt.graphic.attributes['precinct'])
      selectedPrecinct = evt.graphic.attributes['precinct']

      if(layer.graphics.length > 0){
        array.forEach(layer.graphics, function(graphic){
          if(graphic){
            if(graphic['attributes']['precinct'] == selectedPrecinct){
              layer.remove(graphic)
            }
          }
        })  
      }
    },
    highlightSelection: function(layer, evt, outsideSymbol, insideSymbol){
      console.log("Selected precinct: " + evt.graphic.attributes['precinct'])
      highlightGraphic = new esriGraphic(evt.graphic.geometry);
    
      if(evt.graphic.attributes['isInDistrict'] == true){
        highlightGraphic.setSymbol(insideSymbol)
      }
      else {
        highlightGraphic.setSymbol(outsideSymbol)
      }
      highlightGraphic.setAttributes(evt.graphic.attributes)
      layer.add(highlightGraphic)
    },
    queryDistrictPrecincts(){
      //This function queries precincts based on the current district selection
      var query = new Query();
      var queryTask = new QueryTask(this_.mapLayers[1]['url']);
      query.returnGeometry = true;
      query.where = "1=1"
      query.outFields = ["*"];
      queryTask.execute(query, function(results){
        console.log("Precincts returned")
      }, function(e){
        console.log(e)
      }).then(this_.isInDistrict).then(this_.displayPrecincts).then(this_.setPopulationTotals).then(this_.getTotalVoters)
    },
    queryPrecincts: function(evt){
      var query = new Query();
      var queryTask = new QueryTask(this_.mapLayers[1]['url']);
      query.returnGeometry = true;
      query.geometry = evt.mapPoint;
      query.spatialRelationship = Query.SPATIAL_REL_INTERSECTS;
      query.outFields = ["*"];
      queryTask.execute(query, function(results){
      }, function(e){
        console.log(e)
      }).then(this_.selectPrecinct).then(this_.getTotalPopulation)
    },
    isInDistrict: function(results){
      console.log("Selected district: ", this_.selectedDistrict)
      console.log("Precincts returned: ", results.features.length)
      precinctsArray = []
      for(let i = 0; i < results.features.length; i++){
        if(results.features[i].attributes['sum_p0010001'] > 0){
          precinctObj = {
            "attributes": {
              "precinct": results.features[i].attributes['precno'],
              "population": results.features[i].attributes['sum_p0010001'],
              "registeredVoters": results.features[i].attributes['total_vote'],
              "isInDistrict": null
            },
            "geometry": results.features[i].geometry
          }
          
          if(results.features[i].attributes['district'] == this_.selectedDistrict){
            precinctObj['attributes']['isInDistrict'] = true
          }
          else {
            precinctObj['attributes']['isInDistrict'] = false
          }
          precinctsArray.push(precinctObj)
        }
        else{
          console.log("precincts could not be displayed. Something may have went wrong.")
        }
        
      }

      return precinctsArray
      
    },
    setPopulationTotals: function(results){
      for(i in this_.districtPopulationArray){
        if(this_.districtPopulationArray[i]['district'] == this_.selectedDistrict){
          this_.districtPopulation = this_.districtPopulationArray[i]['population']
          this_.totalPopulation = this_.districtPopulationArray[i]['population']
        }
      }

      this_.selectedDistrictPopElem.innerHTML = this_.districtPopulation
      this_.totalPopulationElem.innerHTML = this_.totalPopulation

      variance = this_.calculateVariance(this_.districtPopulation, this_.districtAveragePopulation, this_.selectedDistrictVarianceElem)

      return results
    },
    getTotalVoters: function(results){
      this_.districtVoterPopulation = 0;
      for(var el in results){
        precinctVoterPopulation = results[el]['registeredVoters']
        this_.districtVoterPopulation += precinctVoterPopulation
      }

      //this_.selectedDistrictVoterPopElem.innerHTML = this_.districtVoterPopulation

      return results
    },
    getTotalPopulation: function(evt, eventType){
      if(eventType == "isAdded"){
        if(evt.graphic.attributes['isInDistrict'] == true){
          //console.log("population will be subtracted")
          this_.totalPopulation = this_.totalPopulation - evt.graphic.attributes['population']
        }
        else {
          //console.log("population will be added")
          this_.totalPopulation = this_.totalPopulation + evt.graphic.attributes['population']
        }
      }
      else if(eventType == "isRemoved"){
        if(evt.graphic.attributes["isInDistrict"] == true){
          //console.log("population will be added back to the total population")
          this_.totalPopulation = this_.totalPopulation + evt.graphic.attributes['population']
        }
        else {
          //console.log("population will be subtracted from the total population")
          this_.totalPopulation = this_.totalPopulation - evt.graphic.attributes['population']
        }
      }

      this_.totalPopulationElem.innerHTML = this_.totalPopulation

      variance = this_.calculateVariance(this_.totalPopulation, this_.districtAveragePopulation, this_.totalPopulationVarianceElem)

      return this_.totalPopulation
    },
    calculateVariance: function(totalPopulation, averagePopulation, element){
      console.log("Variance calculated")
      variance = (((totalPopulation - averagePopulation)/averagePopulation) * 100).toFixed(2) +'%'
      element.innerHTML = variance
    },
    displayPrecincts: function(results){      
      for(let i = 0; i < results.length; i++){
        //graphic = new esriGraphic(results[i]['geometry'])
        console.log(results[i]['attributes'])
        textSymbol = new esriTextSymbol(results[i]['attributes']['precinct'])
        textSymbol.setColor(new Color([0,0,0]))
        textSymbol.setAlign(esriTextSymbol.ALIGN_MIDDLE)
        textSymbol.setFont(this_.textSymbolFont)
        console.log(textSymbol)
        graphic = new esriGraphic(results[i]['geometry'])
        var labelGraphic = graphic.clone()
        labelGraphic.setSymbol(textSymbol)

        if(results[i]['attributes']['isInDistrict'] == true){
          graphic.setSymbol(this_.insideSymbol)
        }
        else {
          graphic.setSymbol(this_.outsideSymbol)
        }
        graphic.setAttributes(results[i]['attributes'])
        map.graphics.add(labelGraphic)
        this_.mapLayers[4].add(graphic)
      }
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
    },
    
  });
});