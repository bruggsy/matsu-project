import sys

import datetime
import hashlib
import json
import time

class Contour:
  def __init__(self, spectrum, spectralhash,  clusterid, sizep, sizem, pos, color, score, normscore, imgfile):
    self.spectrum = spectrum
    self.contourid = 'C' + clusterid.split('_')[1] + '-' + imgfile[-25:-20] + '-' + imgfile[-11:-8]
    self.sizep = sizep
    self.sizem = sizem
    self.color = color.split(' ')[-1]
    self.lng = pos[0]
    self.lat = pos[1]
    self.clusterscore = score[0]
    self.contourscore = score[1]
    self.nclusterscore = normscore[0]
    self.ncontourscore = normscore[1]
    self.spectralurl = './spectral-report-' + imgfile.split('/')[-1] + '-' + spectralhash + '.html'


  def __lt__(self, other):
    """ uses normalized scores"""
    if self.nclusterscore == other.nclusterscore:
      return self.ncontourscore < other.ncontourscore
    return self.nclusterscore < other.nclusterscore

  def getContourScore(self):
      return "%.4f" % (self.ncontourscore)

  def getClusterScore(self):
      # Return normalized scores as ints
      return "%d" % (self.nclusterscore)

  def getArea(self, pixels=True):
      if pixels:
          return "%.4f" % (self.sizep)
      else:
          return "%.4f" % (self.sizem)


JSMAP = """
<script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
<script type="text/javascript" src="http://maps.google.com/maps/api/js?v=3.2&amp;sensor=false"></script>
<script type="text/javascript" src="http://openlayers.org/api/OpenLayers.js"></script>
<script type="text/javascript">
var map;
var imageLayer;
var width;
var height;
var registration = %s;

$(function () {
    $("div#graphic").prepend("<div style=\\"width: 600px; height: 400px; border: 2px solid black; margin-left: auto; margin-right: auto; margin-bottom: 20px;\\"><div id=\\"map\\"></div></div>");
    map = new OpenLayers.Map({
        div: "map",
        controls: [
            new OpenLayers.Control.TouchNavigation({dragPanOptions: {enableKinetic: true}}),
            new OpenLayers.Control.Zoom(),
            new OpenLayers.Control.LayerSwitcher(),
            new OpenLayers.Control.ScaleLine(),
            ],
        layers: [new OpenLayers.Layer.OSM("OpenStreetMap", null, {transitionEffect: "resize"}),
                 new OpenLayers.Layer.Google("Google Physical", {type: google.maps.MapTypeId.TERRAIN}),
                 new OpenLayers.Layer.Google("Google Satellite", {type: google.maps.MapTypeId.SATELLITE}),
                ],
        center: new OpenLayers.LonLat(registration["middle-middle"]["lat"], registration["middle-middle"]["lng"]).transform(new OpenLayers.Projection("EPSG:4326"), new OpenLayers.Projection("EPSG:900913")),
        zoom: 8
    });

    width = parseInt($("div#graphic svg image").attr("width"));
    height = parseInt($("div#graphic svg image").attr("height"));

    var bl_lat = registration["bottom-left"]["lng"];
    var bm_lat = registration["bottom-middle"]["lng"];
    var br_lat = registration["bottom-right"]["lng"];
    var ml_lat = registration["middle-left"]["lng"];
    var mm_lat = registration["middle-middle"]["lng"];
    var mr_lat = registration["middle-right"]["lng"];
    var tl_lat = registration["top-left"]["lng"];
    var tm_lat = registration["top-middle"]["lng"];
    var tr_lat = registration["top-right"]["lng"];

    var bl_lng = registration["bottom-left"]["lat"];
    var bm_lng = registration["bottom-middle"]["lat"];
    var br_lng = registration["bottom-right"]["lat"];
    var ml_lng = registration["middle-left"]["lat"];
    var mm_lng = registration["middle-middle"]["lat"];
    var mr_lng = registration["middle-right"]["lat"];
    var tl_lng = registration["top-left"]["lat"];
    var tm_lng = registration["top-middle"]["lat"];
    var tr_lng = registration["top-right"]["lat"];

    var dlngdx = (mr_lng - ml_lng) / width;
    var dlatdy = (tm_lat - bm_lat) / height;
    var dlatdx = (mr_lat - ml_lat) / width;
    var dlngdy = (tm_lng - bm_lng) / height;
    var d2latdy2 = 2.0 * (tm_lat - 2.0*mm_lat + bm_lat) / Math.pow(height, 2);
    var d2lngdx2 = 2.0 * (mr_lng - 2.0*mm_lng + ml_lng) / Math.pow(width, 2);
    var d2lngdy2 = 2.0 * (tm_lng - 2.0*mm_lng + bm_lng) / Math.pow(height, 2);
    var d2latdx2 = 2.0 * (mr_lat - 2.0*mm_lat + ml_lat) / Math.pow(width, 2);
    var d2latdxdy = (tr_lat - br_lat - tl_lat + bl_lat) / (height * width);
    var d2lngdxdy = (tr_lng - br_lng - tl_lng + bl_lng) / (height * width);

    OpenLayers.Projection.addTransform("imageCoords", "EPSG:900913", function (point) {
        var x = (point.x - width/2.0);
        var y = (point.y - height/2.0);
        point.x = mm_lng + dlngdx*x + dlngdy*y + d2lngdx2*Math.pow(x, 2) + d2lngdy2*Math.pow(y, 2) + d2lngdxdy*x*y;
        point.y = mm_lat + dlatdy*y + dlatdx*x + d2latdy2*Math.pow(y, 2) + d2latdx2*Math.pow(x, 2) + d2latdxdy*x*y;
        return OpenLayers.Projection.transform(point, "EPSG:4326", "EPSG:900913");
    });

    imageLayer = new OpenLayers.Layer.Image("RGB Image",
                                            $("div#graphic svg image").attr("xlink:href"),
                                            new OpenLayers.Bounds(
                                                OpenLayers.Projection.transform({"x": 0.0, "y": 0.0}, "imageCoords", "EPSG:900913").x,
                                                OpenLayers.Projection.transform({"x": width, "y": height}, "imageCoords", "EPSG:900913").y,
                                                OpenLayers.Projection.transform({"x": width, "y": height}, "imageCoords", "EPSG:900913").x,
                                                OpenLayers.Projection.transform({"x": 0.0, "y": 0.0}, "imageCoords", "EPSG:900913").y),
                                            new OpenLayers.Size(100, 100),
                                            {isBaseLayer: false,
                                             alwaysInRange: true,
                                             opacity: 0.9,
                                            });

    var lineLayer = new OpenLayers.Layer.Vector("Identified contours");

    $("div#graphic svg path").each(function(index) {
        var points = [];
        var closed = false;
        var stroke = this.style["stroke"];

        var segments = this.pathSegList;
        for (var i = 0, len = segments.numberOfItems;  i < len;  i++) {
            var pathSeg = segments.getItem(i);
            switch (pathSeg.pathSegType) {
                case SVGPathSeg.PATHSEG_MOVETO_ABS:
                case SVGPathSeg.PATHSEG_LINETO_ABS:
                    var point = new OpenLayers.Geometry.Point(pathSeg.x, pathSeg.y);
                    point = OpenLayers.Projection.transform(point, "imageCoords", "EPSG:900913");
                    points.push(point);
                    break;
                case SVGPathSeg.PATHSEG_CLOSEPATH:
                    closed = true;
                    break;
            }
        }

        var line;
        if (closed)
            line = new OpenLayers.Geometry.LinearRing(points);
        else
            line = new OpenLayers.Geometry.LineString(points);

        var lineFeature = new OpenLayers.Feature.Vector(line, null, {strokeColor: stroke, strokeOpacity: 1.0, strokeWidth: 2, fillColor: "none"});
        lineLayer.addFeatures([lineFeature]);
    });

    map.addLayers([imageLayer, lineLayer]);
});

</script>
"""


def writeSprectralHTML(year, daynumber, numBands, imgfile, svg, registration, analytic_name, contourlist, clusterlist, spectraldict, outdir, nc):
  for key in spectraldict.keys():
    now = time.strftime("%c")
    ts = datetime.datetime(year, 1, 1) + datetime.timedelta(daynumber - 1)

    # Write out the HTML
    outfile = open(outdir+'/spectral-report-' + imgfile.split('/')[-1] + '-' + key + '.html', 'w')
    # Print opening HTML tags
    outfile.write( '<html><title>Spectral Report</title>')
    # outfile.write( the css / style
    outfile.write( ' ')
    outfile.write( '<style type="text/css">')
    outfile.write( '        .TFtable{')
    outfile.write( '                width:100%;')
    outfile.write( '                border-collapse:collapse;')
    outfile.write( '                font-size : 77%;')
    outfile.write( '                font-family : Verdana,Helvetica,Arial,sans-serif;')
    outfile.write( '        }')
    outfile.write( '        .TFtable td{')
    outfile.write( '                padding:7px; border:#4e95f4 1px solid;')
    outfile.write( '        }')
    outfile.write( '        /* provide some minimal visual accomodation for IE8 and below */')
    outfile.write( '        .TFtable tr{')
    outfile.write( '                background: #b8d1f3;')
    outfile.write( '        }')
    outfile.write( '        /*  Define the background color for all the ODD background rows  */')
    outfile.write( '        .TFtable tr:nth-child(odd){ ')
    outfile.write( '                background: #b8d1f3;')
    outfile.write( '        }')
    outfile.write( '        /*  Define the background color for all the EVEN background rows  */')
    outfile.write( '        .TFtable tr:nth-child(even){')
    outfile.write( '                background: #dae5f4;')
    outfile.write( '        }')
    outfile.write( ' ')
    outfile.write( ' ')
    outfile.write( '        .Summarytable{')
    outfile.write( '                width:100%;')
    outfile.write( '                border-collapse:collapse;')
    outfile.write( '                font-size : 80%;')
    outfile.write( '                font-family : Courier,Courier new,monospace;')
    outfile.write( '        }')
    outfile.write( '        .Summarytable td{')
    outfile.write( '                padding:7px; border:#4e95f4 1px solid;')
    outfile.write( '        }')
    outfile.write( '</style>')
    outfile.write( ' ')

    outfile.write(  '<body>')

    # Map
    outfile.write(JSMAP % registration)

    # Left Panel
    outfile.write(  '<div style="float:left; width: 35%">')

    outfile.write(  '<p style="font-family: times, serif; font-size:16pt; font-style:bold">')
    outfile.write(  'Matsu Analytic Image Report')
    outfile.write(  '</p>')

    outfile.write(  '<table class="Summarytable">')
    outfile.write(  '<tr><td>Collection Date</td><td>'+  ts.strftime('%Y-%m-%d') +' (day '+ str(daynumber).zfill(3) +')</td></tr>')
    outfile.write(  '<tr><td>Analysis Date</td><td>'+ now + '</td></tr>')
    outfile.write(  '<tr><td align="center" colspan="2">Analytic Environment</td></tr>')
    outfile.write(  '<tr><td>Analytic</td><td>' + analytic_name + '</td></tr>')
    outfile.write(  '<tr><td>Noise Correction Enabled</td><td>' + nc + '</td></tr>')
    outfile.write(  '<tr><td>Summary Stats</td><td>ss-2013-12-r1</td></tr>')
    outfile.write(  '<tr><td>Data Ingest</td><td>populateHDFS-2013-11-r1</td></tr>')
    outfile.write(  '<tr><td>Report Format</td><td>reportContoursR4</td></tr>')
    outfile.write(  '<tr><td align="center" colspan="2">Hyperspectral Image</td></tr>')
    outfile.write(  '<tr><td>Image</td><td>' + imgfile + '</td></tr>')
    outfile.write(  '<tr><td>Number of Bands</td><td>' + str(numBands) + '</td></tr>')
    outfile.write(  '</table>')

    outfile.write(  '<br>')
    outfile.write(  '<br>')
    outfile.write(  '<br>')
    outfile.write( spectraldict[key] )
    outfile.write(  '<br>')
    outfile.write(  '<br>')
    outfile.write(  '<br>')
    outfile.write( '</div>')
    # Right Panel

    outfile.write( '<div id="graphic" style="float:right; width: 65%">')
    outfile.write( svg )
    outfile.write( '</div>')

    outfile.write( '</body></html>')

    outfile.close()


def writeHTML(year, daynumber, numBands, imgfile, svg, registration, analytic_name, contourlist, clusterlist, outdir, nc):

  now = time.strftime("%c")
  ts = datetime.datetime(year, 1, 1) + datetime.timedelta(daynumber - 1)

  # Write out the HTML
  outfile = open(outdir+'/image-report-'+imgfile+'.html', 'w')

  # Print opening HTML tags
  outfile.write( '<html><title>Image Report</title>')

  # outfile.write( the css / style
  outfile.write( ' ')
  outfile.write( '<style type="text/css">')
  outfile.write( '        .TFtable{')
  outfile.write( '                width:100%;')
  outfile.write( '                border-collapse:collapse;')
  outfile.write( '                font-size : 77%;')
  outfile.write( '                font-family : Verdana,Helvetica,Arial,sans-serif;')
  outfile.write( '        }')
  outfile.write( '        .TFtable td{')
  outfile.write( '                padding:7px; border:#4e95f4 1px solid;')
  outfile.write( '        }')
  outfile.write( '        /* provide some minimal visual accomodation for IE8 and below */')
  outfile.write( '        .TFtable tr{')
  outfile.write( '                background: #b8d1f3;')
  outfile.write( '        }')
  outfile.write( '        /*  Define the background color for all the ODD background rows  */')
  outfile.write( '        .TFtable tr:nth-child(odd){ ')
  outfile.write( '                background: #b8d1f3;')
  outfile.write( '        }')
  outfile.write( '        /*  Define the background color for all the EVEN background rows  */')
  outfile.write( '        .TFtable tr:nth-child(even){')
  outfile.write( '                background: #dae5f4;')
  outfile.write( '        }')
  outfile.write( ' ')
  outfile.write( ' ')
  outfile.write( '        .Summarytable{')
  outfile.write( '                width:100%;')
  outfile.write( '                border-collapse:collapse;')
  outfile.write( '                font-size : 80%;')
  outfile.write( '                font-family : Courier,Courier new,monospace;')
  outfile.write( '        }')
  outfile.write( '        .Summarytable td{')
  outfile.write( '                padding:7px; border:#4e95f4 1px solid;')
  outfile.write( '        }')
  outfile.write( '</style>')
  outfile.write( ' ')


  outfile.write(  '<body>')

  #Map
  outfile.write(JSMAP % registration)

  # Left Panel
  outfile.write(  '<div style="float:left; width: 35%">')

  outfile.write(  '<p style="font-family: times, serif; font-size:16pt; font-style:bold">')
  outfile.write(  'Matsu Analytic Image Report')
  outfile.write(  '</p>')

  outfile.write(  '<table class="Summarytable">')
  outfile.write(  '<tr><td>Collection Date</td><td>'+  ts.strftime('%Y-%m-%d') +' (day '+ str(daynumber).zfill(3) +')</td></tr>')
  outfile.write(  '<tr><td>Analysis Date</td><td>'+ now + '</td></tr>')
  outfile.write(  '<tr><td align="center" colspan="2">Analytic Environment</td></tr>')
  outfile.write(  '<tr><td>Analytic</td><td>' + analytic_name + '</td></tr>')
  outfile.write(  '<tr><td>Noise Correction Enabled</td><td>' + nc + '</td></tr>')
  outfile.write(  '<tr><td>Summary Stats</td><td>ss-2013-12-r1</td></tr>')
  outfile.write(  '<tr><td>Data Ingest</td><td>populateHDFS-2013-11-r1</td></tr>')
  outfile.write(  '<tr><td>Report Format</td><td>reportContoursR4</td></tr>')
  outfile.write(  '<tr><td align="center" colspan="2">Hyperspectral Image</td></tr>')
  outfile.write(  '<tr><td>Image</td><td>' + imgfile + '</td></tr>')
  outfile.write(  '<tr><td>Number of Bands</td><td>' + str(numBands) + '</td></tr>')
  outfile.write(  '</table>')

  outfile.write(  '<br>')
  outfile.write(  '<br>')
  outfile.write(  '<br>')
  outfile.write(  '<table class="SummaryTable">')
  outfile.write( '<tr><td> Contour ID</td><td>Cluster Score</td><td>Contour Score</td><td>lat,lng</td><td>Area (Pixels)</td><td>Area (Meters)</td><td>color</td><td>Spectral Signature</td></tr>' )
  for c in contourlist:
    outfile.write( '<tr><td>'+c.contourid+'</td><td>'+c.getClusterScore()+'</td><td>'+c.getContourScore()+'</td><td>' + str(c.lat) + ',' + str(c.lng) + '</td><td>' + c.getArea(True) + '</td><td>' + c.getArea(False) + '</td><td bgcolor='+ c.color + '>COLOR</td><td>' + '<a href="' + c.spectralurl + '">wavelengths</a>' + '</td></tr>' )
  outfile.write( '</table>' )

  outfile.write( '</div>')
  # Right Panel

  outfile.write( '<div id="graphic" style="float:right; width: 65%">')
  outfile.write( svg)
  outfile.write( '</div>')

  outfile.write( '</body></html>')

  outfile.close()


def main(year, daynumber, ifile, analytic_name, outdir, nc):
  infile=open(ifile, 'r')
  for l in infile.readlines():
    contourlist = []
    clusterlist = []
    spectraldict = {}
    f,v = l.split('\t')
    f=f.split('/')[-1] 
    data = json.loads(v)
    for d in data.keys():
      if d == "metadata":
          numBands = len(data[d]['bandNames'])
          try:
              registration = json.dumps(data[d]['geotag']["registration"])
          except KeyError:
              registration = json.dumps(data[d]["registration"])

      if d == "contouredimage":
          svg =  data[d]

      if d == "clusterinfo":
          lst = data[d]
          for dd in lst:
            if dd.startswith("cluster_"):
                cntrs95 = data[d][dd]['contours95']
                for cntrs in cntrs95:
                    spectrum = cntrs['clusterspectrum']
                    spectralhash = hashlib.sha224(spectrum).hexdigest()
                    c = Contour(spectrum, spectralhash, dd, cntrs['areaInPixels'], cntrs['areaInMeters'], cntrs['centroidInLngLat'], cntrs['style'], cntrs['score'], cntrs['normscore'], f)
                    contourlist.append(c)
                    clusterlist.append(dd)
                    spectraldict[spectralhash] = spectrum
                   

    writeSprectralHTML(year, daynumber, numBands, f, svg, registration, analytic_name, contourlist, clusterlist, spectraldict, outdir, nc)
    writeHTML(year, daynumber, numBands, f, svg, registration, analytic_name, contourlist, clusterlist, outdir, nc)


  infile.close()


if __name__ == "__main__":
    year = sys.argv[1]
    daynumber = sys.argv[2]
    analytic_name = sys.argv[3]
    ifile = sys.argv[4]
    outdir = sys.argv[5]
    nc = sys.argv[6]

    main(int(year), int(daynumber), ifile, analytic_name, outdir, nc)

