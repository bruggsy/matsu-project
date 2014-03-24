#!/usr/bin/env python

import sys
import json
import numpy
from collections import defaultdict

def calcHist(hist1, hist2):
    hist1Mean = numpy.mean(hist1)
    hist2Mean = numpy.mean(hist2)

    correlationTop = numpy.sum((hist1 - hist1Mean) * (hist2 - hist2Mean))  
    correlationBottom = numpy.sqrt( numpy.sum( (hist1 - hist1Mean)**2 ) * numpy.sum( (hist2 - hist2Mean)**2 ) )
    histCorrelation = correlationTop / correlationBottom

    histChiSquared = .5 * numpy.sum( (hist1 - hist2)**2 / (hist1 + hist2) )

    return histCorrelation, histChiSquared

def main(stats):
    calcStats = {}
    histStats = defaultdict(list)

    for regionKey in stats:
        for key in stats[regionKey]:
            if key not in ['grayBandHistogram', 'bandNames', 'grayBandRogueBands', 'metadata', 'grayBandHistPeaks', 'grayBandExplainedVariance']:
                if key not in calcStats.keys():
                    calcStats[key] = 0
                calcStats[key] += float(stats[regionKey][key])/len(stats)
            elif key == 'grayBandHistogram':
                histStats[regionKey] = [int(x) for x in stats[regionKey]['grayBandHistogram'][1]]
                calcStats[key] = stats[regionKey][key] #set default final histogram to last value 
            elif key == 'grayBandExplainedVariance':   #set default explained variance to last value 
                calcStats[key] = stats[regionKey][key]
            else:
                pass

        calcStats['outlierImage'] = regionKey          # set default outlier image to last value


    if len(histStats) > 1:
        calcHistStats = {}
        for key1 in histStats.keys():
            calcHistStats[key1] = {}
            for key2 in [x for x in histStats.keys() if x != key1]:
                calcHistStats[key1][key2] = calcHist(numpy.array(histStats[key1]),numpy.array(histStats[key2]))

        maxDist = [0,'']
        for key in calcHistStats:
            y = numpy.array(calcHistStats[key].values())
            thisDist = numpy.mean(y[:,0]/y[:,1])
            calcHistStats[key]["meanDistanceToOtherHistograms"] = thisDist
            if thisDist > maxDist[0]:
                maxDist = [thisDist,key]

        #calcStats[maxDist[1]] = list(numpy.mean(numpy.array([calcHistStats[maxDist[1]][x] for x in calcHistStats[maxDist[1]] if x != 'meanDistanceToOtherHistograms']),axis=0))
        calcStats['grayBandHistogram'] = stats[maxDist[1]]['grayBandHistogram']
        calcStats['outlierImage'] = maxDist[1]
        calcStats['grayBandExplainedVariance'] = map(float,stats[maxDist[1]]['grayBandExplainedVariance'])

    print json.dumps(calcStats)

if __name__ == "__main__":
    statsDict = defaultdict(dict) 
   
    last_key = []
    for line in sys.stdin:
        line = line.rstrip().split('\t')
        groupKey = line[0]
        value = json.loads(line[1])
        regionKey = value["metadata"]["originalDirName"]
        statsDict[regionKey] = value

    main(statsDict)
