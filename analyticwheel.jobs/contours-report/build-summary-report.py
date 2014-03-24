import datetime
import json
import os
import sys
import time

"""
This should be run against the output under contours:
$ python build-summary-report.py 2013 287 2013-287/contours-report/part-00000 
and it generates a single html file
"""

class Contour:
  def __init__(self, clusterid, sizep, sizem, pos, score, normscore, imgfile):
    # This needs to be customized for each file strucute to get uniqueness
    #self.contourid = 'C' + clusterid.split('_')[1] + '-' + imgfile
    # EO1
    self.contourid = 'C' + clusterid.split('_')[1] + '-' + imgfile[-25:-20] + '-' + imgfile[-11:-8]
    self.sizep = sizep
    self.sizem = sizem
    self.lng = pos[0]
    self.lat = pos[1]
    self.clusterscore = score[0]
    self.contourscore = score[1]
    self.nclusterscore = normscore[0]
    self.ncontourscore = normscore[1]
    self.imgfile = imgfile
    self.url = 'overlays/image-report-' + imgfile.split('/')[-1] + '.html'

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


def main(year, daynumber, ifile, analytic_name, outdir, nc):
  filelist = []
  contourlist = []
  clusterlist = []

  # Will get set each iteration below, but to the same value each time.
  avgPixelsRun = 0
  outlier = None

  infile=open(ifile, 'r')
  for l in infile.readlines():

    f,v = l.split('\t')
    #print "Filename ", f.split('/')[-1] 
    filelist.append(f)
    data = json.loads(v)
    for d in data.keys():
        #print d
        if d == "clusterinfo":
          lst = data[d]
          for dd in lst:
            if dd.startswith("cluster_"):
                #print " >>>> ", dd
                cntrs95 = data[d][dd]['contours95']
                #print "   >>>>> ", cntrs95
                for cntrs in cntrs95:
                    c = Contour(dd, cntrs['areaInPixels'], cntrs['areaInMeters'], cntrs['centroidInLngLat'], cntrs['score'], cntrs['normscore'], f)
                    contourlist.append(c)
                    clusterlist.append(dd)

        if d == "summarystatistics":
          #print "Average Number of Pixels per Image in Run ", data[d]['numPixels']
          avgPixelsRun = data[d]['numPixels']
          #print "Image with largest Std. Dev in Run  ", data[d]['outlierImage']
          outlier = data[d]['outlierImage']


  infile.close()

  contourlist.sort(reverse=True)

  #print "Cluster list: ", clusterlist
  #print "Contour list: ", contourlist

  # Build the report summary
  AnalyticName='Hyper-spectral Objects'
  now = time.strftime("%c")
  ts = datetime.datetime(year, 1, 1) + datetime.timedelta(daynumber - 1)

  # Write out the HTML
  threedigitday = str(daynumber).zfill(3) 
  outfile = open(outdir+'/summary-report-'+str(year)+'-'+str(threedigitday)+'.html', 'w')


  # Print opening HTML tags
  outfile.write( '<html><title>Sumary Report</title>' )

  # outfile.write( the css / style 
  outfile.write( ' ' )
  outfile.write( '<style type="text/css">' )
  outfile.write( '        .TFtable{' )
  outfile.write( '                width:50%;' )
  outfile.write( '                border-collapse:collapse;' )
  outfile.write( '                font-size : 77%;' )
  outfile.write( '                font-family : Verdana,Helvetica,Arial,sans-serif;' )
  outfile.write( '        }' )
  outfile.write( '        .TFtable td{' )
  outfile.write( '                padding:7px; border:#4e95f4 1px solid;' )
  outfile.write( '        }' )
  outfile.write( '        /* provide some minimal visual accomodation for IE8 and below */' )
  outfile.write( '        .TFtable tr{' )
  outfile.write( '                background: #b8d1f3;' )
  outfile.write( '        }' )
  outfile.write( '        /*  Define the background color for all the ODD background rows  */' )
  outfile.write( '        .TFtable tr:nth-child(odd){ ' )
  outfile.write( '                background: #b8d1f3;' )
  outfile.write( '        }' )
  outfile.write( '        /*  Define the background color for all the EVEN background rows  */' )
  outfile.write( '        .TFtable tr:nth-child(even){' )
  outfile.write( '                background: #dae5f4;' )
  outfile.write( '        }' )
  outfile.write( ' ' )
  outfile.write( ' ' )
  outfile.write( '        .Summarytable{' )
  outfile.write( '                width:50%;' )
  outfile.write( '                border-collapse:collapse;' )
  outfile.write( '                font-size : 80%;' )
  outfile.write( '                font-family : Courier,Courier new,monospace;' )
  outfile.write( '        }' )
  outfile.write( '        .Summarytable td{' )
  outfile.write( '                padding:7px; border:#4e95f4 1px solid;' )
  outfile.write( '        }' )
  outfile.write( '</style>' )
  outfile.write( ' ' )

  outfile.write( '<body>' )


  outfile.write( '<p style="font-family: times, serif; font-size:16pt; font-style:bold">' )
  outfile.write( 'Analytic Summary Report' )
  outfile.write( '</p>' )

  outfile.write( '<table class="Summarytable">' )
  outfile.write( '<tr><td>Collection Date</td><td>'+  ts.strftime('%Y-%m-%d') +' (day '+ str(threedigitday) +')</td></tr>' )
  outfile.write( '<tr><td>Analysis Date</td><td>'+ now + '</td></tr>' )

  outfile.write( '<tr><td align="center" colspan="2">Analytic Environment</td></tr>' )
  outfile.write( '<tr><td>Analytic</td><td>' + analytic_name + '</td></tr>'   )
  outfile.write( '<tr><td>Noise Correction Enabled</td><td>' + nc + '</td></tr>')
  outfile.write( '<tr><td>Summary Stats</td><td>ss-2013-12-r1</td></tr>'   )
  outfile.write( '<tr><td>Data Ingest</td><td>populateHDFS-2013-11-r1</td></tr>'   )
  outfile.write( '<tr><td>Report Format</td><td>reportContoursR4</td></tr>')


  outfile.write( '<tr><td align="center" colspan="2">Run Summary</td></tr>' )
  outfile.write( '<tr><td>Number of Image</td><td>' + str(len(filelist) ) +'</td></tr>'   )
  outfile.write( '<tr><td>Average Number of Pixels</td><td>'+ "%d" % (avgPixelsRun) +' </td></tr>'   )
  outfile.write( '<tr><td>Image with Largest Variance</td><td>'+ outlier.split("/")[-1] + '</td></tr>'   )


  outfile.write( '<tr><td align="center" colspan="2">Images</td></tr>' )
  for f in filelist:
    url = 'overlays/image-report-' + f.split('/')[-1] + '.html'
    outfile.write( '<tr><td align="center" colspan="2"><a href="' + url + '">'+ f + '</a>' + '</td></tr>' )
  outfile.write( '</table>' )

  outfile.write( '<br>' )
  outfile.write( '<br>' )
  outfile.write( '<br>' )
  outfile.write( '<br>' )

  outfile.write( '<table class="TFtable">' )

  # load analytic name
  outfile.write( '<tr><td></td><td align="center" colspan="5">'+AnalyticName+'</td></tr>' )
  outfile.write( '<tr><td>Rank</td><td>Object Name</td><td>Cluster Score</td><td>Contour Score</td><td>Location (lng/lat)</td><td>Image</td></tr>' )
  # Print the content of the table, line by line, for the top 10 objects
  i=1
  for c in contourlist:
    #c = contourlist[i]
    if i > 10: break
    outfile.write( '<tr><td>'+str(i)+'</td><td>'+c.contourid+'</td><td>'+c.getClusterScore()+'</td><td>'+c.getContourScore()+'</td><td>' + str(c.lat) + ',' + str(c.lng) + '</td><td>'+ '<a href="' + c.url + '">'+ c.imgfile + '</a>' + '</td></tr>' )
    i+=1
  outfile.write( '</table>' )

  outfile.write( '<p style="font-family: times, serif; font-size:12pt; font-style:italic">' )
  outfile.write( 'See the individual reports for hyperspectral details' )
  outfile.write( '</p>' )

  # Print closing HTML tags
  outfile.write( '</body>' )
  outfile.write( '</html>' )

  outfile.close()


if __name__ == "__main__":
    year = sys.argv[1]
    daynumber = sys.argv[2]
    analytic_name = sys.argv[3]
    ifile = sys.argv[4]
    outdir = sys.argv[5]
    nc = sys.argv[6]

    main(int(year), int(daynumber), ifile, analytic_name, outdir, nc)


