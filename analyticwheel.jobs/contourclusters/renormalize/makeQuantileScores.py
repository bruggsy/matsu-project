#!/usr/bin/env python


import sys
import json
import numpy

import time
import datetime

def setPercentiles(scores,qscore):

    if qscore is None:
        qscore = {1000: 100, 900: 95, 800: 90, 700:85, 600:80, 500:75, 400:70, 300:65, 200:60, 100:55, 0:50} 

    for key,value in qscore.iteritems():
       qscore[key] = [numpy.percentile(scores,value),value]

    return qscore

def makeCut(scores, topCut):
    
    for i in xrange(len(scores)):
        if scores[i] > topCut:
            scores[i] = topCut

    return scores

def setCut(scores,topscorecut):

    return numpy.percentile(scores,topscorecut)


def makeScoreMap(dictscore,topscorecut,quantileMap):


    topcut = setCut(dictscore.keys(),topscorecut)
    scores = makeCut(dictscore.keys(),topcut)

    quantilemap = setPercentiles(scores,quantileMap)

    quantilemap['version number'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    print json.dumps(quantilemap)



if __name__=="__main__":


    # If no top score is set, default will be 99th percentile
    if len(sys.argv) > 1:
        if type(sys.argv[1]) is int:
            topScoreCut = float(sys.argv[1])
        else:
            topScoreCut = 99
    else:
        topScoreCut = 99   


    # If no quantile map is set, use default defined in method setPercentiles
    if len(sys.argv) > 2:
        if type(sys.argv[2]) is dict:
            quantileMap = json.load(open(sys.argv[2],'r'))
        else:
            quantileMap = None
    else:
        quantileMap = None

  
    dict_score = {}
    for line in sys.stdin.readlines():
        line = line.rstrip().split('\t')
        analyticscore = json.loads(line[1])
        for key, clusterValue in analyticscore.iteritems():
            if "metadata" not in key:
                for i in xrange(len(clusterValue['contours95'])):
                    dict_score[clusterValue['contours95'][i]['score'][0]] = key
 
    
    makeScoreMap(dict_score,topScoreCut,quantileMap)
