<html>
  <head>
    <meta http-equiv="content-type" content="text/html;charset=UTF-8" />
    <title>SimpleWMS</title>
    <style type="text/css">
      .spacer { margin-left: 10px; margin-right: 10px; }
      .layer_checkbox { margin-left: 20px; margin-top: 2px; margin-bottom: 2px; }
      table { table-layout: fixed; } 
      .header { font-weight: bold; vertical-align: bottom; }
      .row { max-width: 200px; overflow: hidden; padding-left: 5px; padding-right: 5px; }
      .even { background: #e7e7e7; }
      .odd { background: #f3f3f3; }
      .cell { text-align: left; }
      .clickableblue:hover { background: #c2d0f2; }
      .clickablered:hover { background: #fcb4ae; }
    </style>
    <script type="text/javascript" src="http://maps.googleapis.com/maps/api/js?key=AIzaSyAVNOfpLX6KdByplQxeMH1kuPZcYWBmz3c&sensor=false"></script>
    <link rel="stylesheet" href="css/black-tie/jquery-ui-1.8.23.custom.css">
    <script type="text/javascript" src="js/jquery-1.8.0.min.js"></script>
    <script type="text/javascript" src="js/jquery-ui-1.8.23.custom.min.js"></script>
    <script type="text/javascript">
// <![CDATA[

<%! 
public String giveMeSomething(String name, String defaultValue, HttpServletRequest request) {
    String x = request.getParameter(name);
    if (x == null) { return defaultValue; }
    else { return x; }
}   
%>

var map;
var circle;

var overlays = {};
var lat = <%= giveMeSomething("lat", "40.183", request) %>;
var lng = <%= giveMeSomething("lng", "94.312", request) %>;
var z = <%= giveMeSomething("z", "9", request) %>;
var layers = ["RGB"];

var points = {};
var dontReloadPoints = {};
var oldsize;
var crossover = 4;
var showPoints = true;

var alldata;

var stats;
var stats_depth = -1;
var stats_numVisible = 0;
var stats_numInMemory = 0;
var stats_numPoints = 0;

var map_canvas;
var sidebar;

var minUserTime;
var maxUserTime;

Number.prototype.pad = function(size) {
    if (typeof(size) !== "number") { size = 2; }
    var s = String(this);
    while (s.length < size) {
        s = "0" + s;
    }
    return s;
}

// Array.prototype.inclusiveRange = function(low, high) {
//     var i, j;
//     for (i = low, j = 0;  i <= high;  i++, j++) {
//         this[j] = i;
//     }
//     return this;
// }

Object.size = function(obj) {
    var size = 0;
    for (var key in obj) {
        if (obj.hasOwnProperty(key)) { size++; }
    }
    return size;
};

function isNumber(num) {
    return (typeof num == "string" || typeof num == "number") && !isNaN(num - 0) && num !== "";
};

function tileIndex(depth, longitude, latitude) {
    if (Math.abs(latitude) > 90.0) { alert("one"); }
    longitude += 180.0;
    latitude += 90.0;
    while (longitude <= 0.0) { longitude += 360.0; }
    while (longitude > 360.0) { longitude -= 360.0; }
    longitude = Math.floor(longitude/360.0 * Math.pow(2, depth+1));
    latitude = Math.min(Math.floor(latitude/180.0 * Math.pow(2, depth+1)), Math.pow(2, depth+1) - 1);
    return [depth, longitude, latitude];
}

function tileName(depth, longIndex, latIndex, layer) {
    return "T" + depth.pad(2) + "-" + longIndex.pad(5) + "-" + latIndex.pad(5) + "-" + layer;
}

function tileCorners(depth, longIndex, latIndex) {
    var longmin = longIndex*360.0/Math.pow(2, depth+1) - 180.0;
    var longmax = (longIndex + 1)*360.0/Math.pow(2, depth+1) - 180.0;
    var latmin = latIndex*180.0/Math.pow(2, depth+1) - 90.0;
    var latmax = (latIndex + 1)*180.0/Math.pow(2, depth+1) - 90.0;
    return new google.maps.LatLngBounds(
        new google.maps.LatLng(latmin, longmin),
        new google.maps.LatLng(latmax, longmax));
}

function doresize() {
    map_canvas.style.width = window.innerWidth - sidebar.offsetWidth - 20;
    var height = window.innerHeight - stats.offsetHeight - 20;
    map_canvas.style.height = height;
    sidebar.style.height = height - 10;
}

window.onresize = doresize;

function initialize() {
    var nodeList = document.querySelectorAll("input.layer-checkbox");
    for (var i in nodeList) {
	if (nodeList[i].type == "checkbox") {
            nodeList[i].checked = (layers.indexOf(nodeList[i].id.substring(6)) != -1);
	}
    }
    document.getElementById("show-points").checked = showPoints;

    getTable();

    var latLng = new google.maps.LatLng(lat, lng);
    var options = {zoom: z, center: latLng, mapTypeId: google.maps.MapTypeId.TERRAIN};
    map = new google.maps.Map(document.getElementById("map_canvas"), options);
    google.maps.event.addListener(map, "bounds_changed", getEverything);

    circle = new google.maps.MarkerImage("circle.png", new google.maps.Size(18, 18), new google.maps.Point(0, 0), new google.maps.Point(9, 9), new google.maps.Size(18, 18));
    oldsize = 0;

    stats = document.getElementById("stats");
    map_canvas = document.getElementById("map_canvas");
    sidebar = document.getElementById("sidebar");
    doresize();
    sidebar.addEventListener("DOMAttrModified", doresize);

    minUserTime = 1230789600;    // Jan 1, 2009 00:00:00
    maxUserTime = 1357020000;    // Jan 1, 2013 00:00:00

    $(function() {
	$( "#slider-time" ).slider({
	    range: true,
	    min: minUserTime,
	    max: maxUserTime,
	    values: [minUserTime, maxUserTime],
	    slide: function( event, ui ) {
		minUserTime = ui.values[0];
		maxUserTime = ui.values[1];
	    },
	    stop: function( event, ui ) {
		for (var key in overlays) {
		    overlays[key].setMap(null);
		    delete overlays[key];
		}
		overlays = {};

		for (var key in points) {
		    points[key].setMap(null);
		    delete points[key];
		}
		points = {};
		dontReloadPoints = {};

		getEverything();
	    }
	});
    });
}

function getTable() {
    var xmlhttp = new XMLHttpRequest();
    xmlhttp.onreadystatechange = function() {
	if (xmlhttp.readyState == 4  &&  xmlhttp.status == 200) {
	    if (xmlhttp.responseText != "") {
		alldata = JSON.parse(xmlhttp.responseText)["data"];
		drawTable();
	    }
	}
    }
    xmlhttp.open("GET", "../TileServer/getTile?command=points", true);
    xmlhttp.send();
}

function drawTable(sortfield, numeric, increasing) {
    if (sortfield != null) {
	var inmetadata = (sortfield != "latitude"  &&  sortfield != "longitude"  &&  sortfield != "time");
	alldata.sort(function(a, b) {
            var aa, bb;
	    if (inmetadata) {
		aa = a["metadata"][sortfield];
	       	bb = b["metadata"][sortfield];
	    }
	    else {
		aa = a[sortfield];
		bb = b[sortfield];
	    }

	    if (numeric) {
		if (increasing) {
		    return aa - bb;
		}
		else {
		    return bb - aa;
		}
	    }
	    else {
		if (increasing) {
		    return aa > bb;
		}
		else {
		    return bb > aa;
		}
	    }
	});
    }

    var fields = ["latitude", "longitude", "acquisition time"];
    var nonNumericFields = [];
    var rowtexts = [];
    
    for (var i in alldata) {
	var evenOdd = "even";
	if (i % 2 == 1) { evenOdd = "odd"; }

	var row = alldata[i];
	for (var m in row["metadata"]) {
            if (fields.indexOf(m) == -1) {
		fields.push(m);
	    }
	}

	var func = "map.setCenter(new google.maps.LatLng(" + row["latitude"] + ", " + row["longitude"] + ")); map.setZoom(13);";

	var rowtext = "<tr id=\"table-" + row["identifier"] + "\" class=\"row " + evenOdd + " clickablered\" onmouseup=\"" + func + "\">";

	for (var fi in fields) {
	    var f = fields[fi];
	    var s;
	    if (fi < 2) {
		s = row[f];
	    }
	    else if (fi == 2) {
		var d = new Date(1000 * row["time"]);
		d.setMinutes(d.getMinutes() + d.getTimezoneOffset());  // get rid of any local timezone correction on the client's machine!
		s = d.getFullYear() + "-" + (d.getMonth() + 1).pad(2) + "-" + d.getDate().pad(2) + " " + d.getHours().pad(2) + ":" + d.getMinutes().pad(2);
	    }
	    else {
		s = row["metadata"][f];
	    }
	    rowtext += "<td class=\"cell\">" + s + "</td>";

	    if (fi != 2  &&  !isNumber(s)  &&  nonNumericFields.indexOf(f) == -1) {
		nonNumericFields.push(f);
	    }
	}
	rowtext += "</tr>";

	rowtexts.push(rowtext);
    }

    var headerrow = "<tr class=\"row header\">";
    for (var fi in fields) {
	var f = fields[fi];
	var func = "drawTable('" + f + "', " + (nonNumericFields.indexOf(f) == -1) + ", " + (!increasing) + ");";
	headerrow += "<td class=\"cell clickableblue\" onmouseup=\"" + func + "\">" + f + "</td>";
    }
    headerrow += "</tr>\n";
    
    document.getElementById("table-here").innerHTML = "<table>\n" + headerrow + rowtexts.join("\n") + "\n</table>";
}

function getEverything() {
    getOverlays();
    if (showPoints) { getLngLatPoints(); }
    else { updateStatus(); }
}

function toggleState(name, objname) {
    var obj = document.getElementById(objname);
    var newState = !(obj.checked);
    obj.checked = newState;

    var i = layers.indexOf(name);

    if (newState  &&  i == -1) {
	layers.push(name);
    }
    else if (!newState  &&  i != -1) {
	layers.splice(i, 1);

	for (var key in overlays) {
            if (key.substring(16) == name) {
		overlays[key].setMap(null);
		delete overlays[key];
	    }
	}
    }

    getOverlays();
}

function togglePoints(objname) {
    var obj = document.getElementById(objname);
    showPoints = !(obj.checked);
    obj.checked = showPoints;

    if (showPoints) {
	getLngLatPoints();
    }
    else {
	for (var key in points) {
            points[key].setMap(null);
	    delete points[key];
	}
	points = {};
	dontReloadPoints = {};
	oldsize = -2;
	stats_numPoints = 0;
	updateStatus();
    }
}

function getOverlays() {
    var bounds = map.getBounds();
    if (!bounds) { return; }

    var depth = map.getZoom() - 2;
    if (depth > 10) { depth = 10; }

    var longmin = bounds.getSouthWest().lng();
    var longmax = bounds.getNorthEast().lng();
    var latmin = bounds.getSouthWest().lat();
    var latmax = bounds.getNorthEast().lat();

    [depth, longmin, latmin] = tileIndex(depth, longmin, latmin);
    [depth, longmax, latmax] = tileIndex(depth, longmax, latmax);

    var depthPad = depth.pad(2);
    for (var key in overlays) {
        if ((key[1] + key[2]) != depthPad) {
            overlays[key].setMap(null);
            delete overlays[key];
        }
    }

    stats_depth = depth;

    stats_numVisible = 0;
    var numAdded = 0;
    for (var i in layers) {
	for (var longIndex = longmin;  longIndex <= longmax;  longIndex++) {
            for (var latIndex = latmin;  latIndex <= latmax;  latIndex++) {
	    	var key = tileName(depth, longIndex, latIndex, layers[i]);
		if (!(key in overlays)) {
                    var overlay = new google.maps.GroundOverlay("../TileServer/getTile?command=images&key=" + key + "&timemin=" + minUserTime + "&timemax=" + maxUserTime, tileCorners(depth, longIndex, latIndex));
                    overlay.setMap(map);
                    overlays[key] = overlay;
                    numAdded++;
		}
		stats_numVisible++;
            }
	}
    }

    stats_numInMemory = 0;
    for (var key in overlays) {
        stats_numInMemory++;
    }
}

function getLngLatPoints() {
    var bounds = map.getBounds();
    if (!bounds) { return; }

    var depth = map.getZoom() - 2;
    var size = 0;
    if (depth <= 9) {
	circle.size = new google.maps.Size(18, 18);
	circle.scaledSize = new google.maps.Size(18, 18);
	circle.anchor = new google.maps.Point(9, 9);
	if (depth <= crossover) {
            size = -1;
	}
    }
    else {
	size = Math.pow(2, depth - 10);
	circle.size = new google.maps.Size(36 * size, 36 * size);
	circle.scaledSize = new google.maps.Size(36 * size, 36 * size);
	circle.anchor = new google.maps.Point(18 * size, 18 * size);
    }

    if (oldsize != size) {
	for (var key in points) {
            points[key].setMap(null);
	    delete points[key];
	}
	points = {};
	dontReloadPoints = {};
    }
    oldsize = size;

    var longmin = bounds.getSouthWest().lng();
    var longmax = bounds.getNorthEast().lng();
    var latmin = bounds.getSouthWest().lat();
    var latmax = bounds.getNorthEast().lat();

    [depth, longmin, latmin] = tileIndex(10, longmin, latmin);
    [depth, longmax, latmax] = tileIndex(10, longmax, latmax);

    var key = "" + depth + "-" + longmin + "-" + latmin + "-" + longmax + "-" + latmax;
    for (var oldkey in dontReloadPoints) {
	if (key == oldkey) {
	    return;
	}
    }
    dontReloadPoints[key] = true;

    var url;
    if (size != -1) {
	url = "../TileServer/getTile?command=points&longmin=" + longmin + "&longmax=" + longmax + "&latmin=" + latmin + "&latmax=" + latmax;
    }
    else {
	url = "../TileServer/getTile?command=points&longmin=" + longmin + "&longmax=" + longmax + "&latmin=" + latmin + "&latmax=" + latmax + "&groupdepth=" + crossover;
    }

    var xmlhttp = new XMLHttpRequest();
    xmlhttp.onreadystatechange = function() {
	if (xmlhttp.readyState == 4  &&  xmlhttp.status == 200) {
	    if (xmlhttp.responseText != "") {
		var data = JSON.parse(xmlhttp.responseText)["data"];
		for (var i in data) {
	    	    var identifier = data[i]["identifier"];
		    if (!(identifier in points)) {
			points[identifier] = new google.maps.Marker({"position": new google.maps.LatLng(data[i]["latitude"], data[i]["longitude"]), "map": map, "flat": true, "icon": circle});

			google.maps.event.addListener(points[identifier], "click", function(ident) { return function() {
			    var obj = document.getElementById("table-" + ident);
			    obj.style.background = "#ffff00";
			    sidebar.scrollTop = obj.offsetTop;

			    var countdown = 10;
			    var state = true;
			    var callme = function() {
				if (state) {
				    obj.style.background = null;
				    state = false;
				}
				else {
				    obj.style.background = "#ffff00";
				    state = true;
				}

				countdown--;
				if (countdown >= 0) { setTimeout(callme, 200); }
			    };
			    setTimeout(callme, 200);

			} }(identifier));
		    }
		}
	    }

	    stats_numPoints = Object.size(points);
	    updateStatus();
	}
    }
    xmlhttp.open("GET", url, true);
    xmlhttp.send();
}

function updateStatus() {
    stats.innerHTML = "<span class='spacer'>Zoom depth: " + stats_depth + "</span><span class='spacer'>Tiles visible: " + stats_numVisible + "</span><span class='spacer'>Tiles in your browser's memory: " + stats_numInMemory + " (counting empty tiles)</span><span class='spacer'>Points: " + stats_numPoints + "</span>";
}

// ]]>
    </script>
  </head>
  <body onload="initialize();" style="width: 100%; margin: 0px;">

  <div id="map_canvas" style="position: fixed; top: 5px; right: 5px; width: 100px; height: 100px; float: right; border: 1px solid black;"></div>
  <div id="sidebar" style="position: fixed; top: 5px; left: 5px; width: 300px; height: 100px; vertical-align: top; resize: horizontal; float: left; background: white; border: 1px solid black; padding: 5px; overflow-x: hidden; overflow-y: scroll;">

<h3 style="margin-top: 0px;">Timespan</h3>
<div style="position: relative; height: 20px; top: -10px;">
<div style="position: absolute; top: 0px; left: 10px; font-size: 11pt;">2009</div>
<div style="position: absolute; top: 0px; left: 50px; font-size: 11pt;">2010</div>
<div style="position: absolute; top: 0px; left: 90px; font-size: 11pt;">2011</div>
<div style="position: absolute; top: 0px; left: 130px; font-size: 11pt;">2012</div>
<div style="position: absolute; top: 0px; left: 170px; font-size: 11pt;">2013</div>
<div style="position: absolute; top: 20px; left: 25px; width: 163px;"><div id="slider-time"></div></div>
</div>

<h3 style="margin-top: 20px;">Layers</h3>
<form onsubmit="return false;">
<p class="layer_checkbox" onclick="toggleState('RGB', 'layer-RGB');"><label for="layer-RGB" onclick="toggleState('RGB', 'layer-RGB');"><input id="layer-RGB" class="layer-checkbox" type="checkbox" checked="true"> Canonical RGB</label>
<p class="layer_checkbox" onclick="toggleState('CO2', 'layer-CO2');"><label for="layer-CO2" onclick="toggleState('CO2', 'layer-CO2');"><input id="layer-CO2" class="layer-checkbox" type="checkbox" checked="true"> Canonical CO2</label>
</form>

<h3 style="margin-bottom: 0px;">Points</h3>
<form onsubmit="return false;">
<p class="layer_checkbox" onclick="togglePoints('show-points');"><label for="show-points"><input id="show-points" type="checkbox" checked="true"> Show points</label>
<p id="table-here" class="layer_checkbox" style="margin-top: 10px;"></p>

</form>

</div>

  <div id="stats" style="position: fixed; bottom: 5px; width: 100%; text-align: center;"><span style="color: white;">No message</span></div>

  </body>
</html>
