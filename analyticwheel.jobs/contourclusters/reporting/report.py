#!/usr/bin/env python

import sys
import numpy
import json
from collections import defaultdict

# Example of running this:
# cat clustercontour.json | ./report.py


def clusterToColor(replacementpixels):

    styledict = {}
    for polygon in replacementpixels:
        styledict[polygon["cluster"]] = polygon["style"]

    return styledict


def clusterToQuantileScore(score):
   
    qmap = json.load(open('normconfig','r')) 

    versionNumber = qmap['version number']
    del qmap['version number']
  
    normScoreList = sorted([int(x) for x in qmap])
    rawScoreList = sorted([float(qmap[x][0]) for x in qmap])

 
    if score > max(rawScoreList):
        normScore = max(normScoreList)
    elif score < min(rawScoreList):
        normScore = min(normScoreList)
    else:
        for i in xrange(1,len(rawScoreList)):
            if score < rawScoreList[i]:
                normScore = normScoreList[i-1] + (score - rawScoreList[i-1])*(normScoreList[i] - normScoreList[i-1])/(rawScoreList[i] - rawScoreList[i-1])
                break

    return normScore, versionNumber

def imageReport(clusterdict,replacementpixels,clusterspectra):
    clusterinfo = {}
    contourCount = 0
    metadata = clusterdict['metadata']
    del clusterdict['metadata']

    styleDict = clusterToColor(replacementpixels)

    for clusterKey,clusterValue in sorted(clusterdict.items(),key=lambda x: int(x[0].strip('cluster_'))):    
        clusterinfo[clusterKey] = {}
        clusterinfo[clusterKey]['contours95'] = []
        if len(clusterValue['contours95']) > 0:
            for i in xrange(len(clusterValue["contours95"])):               
                clusterinfo[contourCount] = [round(clusterValue['contours95'][i]['areaInMeters'],3),(round(clusterValue['contours95'][i]['centroidInLngLat'][0],5),round(clusterValue['contours95'][i]['centroidInLngLat'][1],5))]
                normscore, versionnumber = clusterToQuantileScore(clusterValue['contours95'][i]['score'][0])
                clusterinfo[clusterKey]['contours95'].append({'areaInPixels': clusterValue['contours95'][i]['areaInPixels'], 'areaInMeters':clusterValue['contours95'][i]['areaInMeters'], 'centroidInLngLat': clusterValue['contours95'][i]['centroidInLngLat'], 'score': clusterValue['contours95'][i]['score'],'normscore': [normscore,clusterValue['contours95'][i]['score'][1]],'score version number': versionnumber,'style': styleDict[clusterKey],'clusterspectrum': clusterspectra[clusterKey]})
                contourCount += 1

    return clusterinfo, metadata


def genSummaryStats(infile):
    SummaryStats = []
    SummaryStats.append(int(infile['numPixels']))
    SummaryStats.append(round(float(infile['grayBandMean']),1))
    SummaryStats.append(round(float(infile['grayBandSigma']),1))
    SummaryStats.append(str(round(100*float(infile['grayBandPlusOneSigma']),0)) + '%')
    SummaryStats.append(str(round(100*float(infile['grayBandMinusOneSigma']),0)) + '%')
    SummaryStats.append(''.join([str(i + 1) + ': ' + str(round(100*float(v),1)) + '%,  ' for i,v in enumerate(infile['grayBandExplainedVariance'])]))
    SummaryStats.append(infile["outlierImage"].split('/')[-1])

    return SummaryStats


def fileToStr(fileName): # NEW
    """Return a string containing the contents of the named file."""
    fin = open(fileName);
    contents = fin.read();
    fin.close()
    return contents


def main(imagekey,clusterinfo,imagesvg,replacementpixels,clusterspectra): 
    clusternewinfo, Metadata = imageReport(clusterinfo,replacementpixels,clusterspectra)

    reportimage = imagesvg

    summarystats = json.load(open('summarystatistics','r'))   

    print '%s\t%s' %  (imageKey,json.dumps({"metadata": Metadata, "clusterinfo": clusternewinfo, "contouredimage": imagesvg, "summarystatistics": summarystats}))


if __name__=="__main__":


    #the clustercontours file contains key tab json
    #of the results from the contour clustering algorithm
    contour_data = open('clustercontours','r')
    contour_dict = {}
    for line in contour_data.readlines():
        key = line.rstrip().split('\t')[0]
        value = line.rstrip().split('\t')[1]
        contour_dict[key] = json.loads(value)



    for line in sys.stdin.readlines():
        line = line.rstrip().split('\t')
        imageKey = line[0]
        sys.stderr.write("This is the key: %r\n" % imageKey)
        data = json.loads(line[1])
        replacementPixels = data["replacement pixels"]
        clusterSpectra = data["cluster spectra"]
        imageSvg = data["contoured image"]


        main(imageKey,contour_dict[imageKey],imageSvg,replacementPixels,clusterSpectra)
