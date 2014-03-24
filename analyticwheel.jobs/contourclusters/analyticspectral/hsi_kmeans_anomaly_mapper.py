#!/usr/bin/env python

import sys
import numpy
import json
import random

from collections import defaultdict

import utilities
import binaryhadoop


def makeContours(xcoordinates,ycoordinates,width,height,binsize): 

    getLngLat = utilities.makeGetLngLat(metadata)
    getMeters = utilities.makeGetMeters(metadata)

    # make the 2d histogram
    clusterdata, xedges, yedges = numpy.histogram2d(xcoordinates, ycoordinates, bins=(int(width/binsize), int(height/binsize)), range=((0, width), (0, height)))
    if len(xcoordinates) == 0:
        clusterdata = numpy.zeros((int(width/binsize), int(height/binsize)), dtype=numpy.dtype(float))

    # make contours for the three levels; the contour polygons are expressed in pixel-index coordinates (not lng/lat or meters)
    contoursMin = utilities.contours(clusterdata, xedges, yedges, 0.5, interpolate=True, smooth=True)
    cutLevel50 = utilities.cutLevel(clusterdata, 50.0)
    contours50 = utilities.contours(clusterdata, xedges, yedges, cutLevel50, interpolate=True, smooth=True)
    cutLevel95 = utilities.cutLevel(clusterdata, 95.0)
    contours95 = utilities.contours(clusterdata, xedges, yedges, cutLevel95, interpolate=True, smooth=True)

    # construct output data that includes the polygons in lng,lat coordinates, circumferences in meters, and areas in meters^2
    clusterData = {"contoursMin": [], "contours50": [], "contours95": [],
                        "numberOfLabeledPixels": len(xcoordinates), "cutLevel50": cutLevel50, "cutLevel95": cutLevel95}

    for polygon in contoursMin:

        lnglatPolygon = utilities.convert(polygon, getLngLat)
        metersPolygon = utilities.convert(lnglatPolygon, getMeters)

        data = {"rowcolpolygon": polygon, "lnglatpolygon": lnglatPolygon, "areaInPixels": numpy.abs(utilities.area(polygon)), "circumferenceInMeters": numpy.abs(utilities.circumference(metersPolygon)), "areaInMeters": numpy.abs(utilities.area(metersPolygon)), "centroidInLngLat": utilities.centroid(lnglatPolygon)}
        clusterData["contoursMin"].append(data)

    for polygon in contours50:

        lnglatPolygon = utilities.convert(polygon, getLngLat)
        metersPolygon = utilities.convert(lnglatPolygon, getMeters)

        data = {"rowcolpolygon": polygon, "lnglatpolygon": lnglatPolygon, "areaInPixels": numpy.abs(utilities.area(polygon)), "circumferenceInMeters": numpy.abs(utilities.circumference(metersPolygon)), "areaInMeters": numpy.abs(utilities.area(metersPolygon)), "centroidInLngLat": utilities.centroid(lnglatPolygon)}
        clusterData["contours50"].append(data)

    for polygon in contours95:

        lnglatPolygon = utilities.convert(polygon, getLngLat)
        metersPolygon = utilities.convert(lnglatPolygon, getMeters)

        data = {"rowcolpolygon": polygon, "lnglatpolygon": lnglatPolygon, "areaInPixels": numpy.abs(utilities.area(polygon)), "circumferenceInMeters": numpy.abs(utilities.circumference(metersPolygon)), "areaInMeters": numpy.abs(utilities.area(metersPolygon)), "centroidInLngLat": utilities.centroid(lnglatPolygon)}
        clusterData["contours95"].append(data)

    return clusterData


def find_regions(array):

    x_dim = array.shape[0]
    y_dim = array.shape[1]

    array_region = numpy.zeros(array.shape)
    equivalences = defaultdict(dict)
    n_regions = 0
    #first pass. find regions.
    ind=numpy.where(array)
    for x,y in zip(ind[0],ind[1]):

        # get the region number from all surrounding cells including diagonals (8) or create new region                        
        xMin=numpy.maximum(x-1,0)
        xMax=numpy.minimum(x+1,x_dim-1)
        yMin=numpy.maximum(y-1,0)
        yMax=numpy.minimum(y+1,y_dim-1)
 
        max_region=numpy.max(array_region[xMin:xMax+1,yMin:yMax+1])
 
        if max_region > 0:
            #a neighbour already has a region, new region is the smallest > 0
            new_region = array_region[xMin:xMax+1,yMin:yMax+1]
            new_region = numpy.min(new_region[new_region>0])
        else:
            n_regions += 1
            new_region = n_regions
 
        array_region[x,y] = new_region

    for i in range(1000):
        array_region1 = numpy.copy(array_region)
        for x,y in zip(ind[0],ind[1]):
            xMin=numpy.maximum(x-1,0)
            xMax=numpy.minimum(x+1,x_dim-1)
            yMin=numpy.maximum(y-1,0)
            yMax=numpy.minimum(y+1,y_dim-1)
            array = array_region[xMin:xMax+1,yMin:yMax+1]
            array_region[xMin:xMax+1,yMin:yMax+1][numpy.where(array>0)] = numpy.min(array[array>0])
        if numpy.abs(array_region - array_region1).sum() == 0:
            break

    unique_array_labels = numpy.unique(array_region)
    relabel = 0
    for unq in unique_array_labels:
        array_region[numpy.where(array_region==unq)] = relabel
        relabel += 1
     
    return numpy.array(array_region,dtype=numpy.int)


def selectClusters(clusterNumber,array,lowerClusterLimit,upperClusterLimit):
    connected_regions = find_regions(array)
    passing_regions = []
    for region in numpy.unique(connected_regions):
        clusterSize = numpy.sum(connected_regions==region)
        if (clusterSize >= lowerClusterLimit) and (clusterSize <= upperClusterLimit):
            cluster_regions = numpy.hstack((numpy.vstack(numpy.where(connected_regions==region)).T,clusterNumber*numpy.ones((clusterSize,1)),region*numpy.ones((clusterSize,1))))
            passing_regions.append(cluster_regions)
    if len(passing_regions) > 0:
        passing_regions = numpy.vstack(tuple(passing_regions))

    return numpy.array(passing_regions)

def kmeans(clusterCenters, dataset, numberOfIterations=10, allChangeThreshold=1e-2, halfChangeThreshold=1e-3):

    for counter in xrange(numberOfIterations):
        bestClusterIndex = None
        bestClusterDistance = None

        sys.stderr.write("reporter:status:still clustering\n")

        for clusterIndex in xrange(clusterCenters.shape[0]):
            distance = numpy.sqrt(numpy.square(dataset - clusterCenters[clusterIndex,:]).sum(axis=1))

            if bestClusterIndex is None:
                bestClusterIndex = numpy.zeros(distance.shape, dtype=numpy.int32)
                bestClusterDistance = distance

            else:
                better = (distance < bestClusterDistance)
                bestClusterIndex[better] = clusterIndex
                bestClusterDistance[better] = distance[better]

        changes = []
        for clusterIndex in xrange(clusterCenters.shape[0]):
            selection = (bestClusterIndex == clusterIndex)
            denom = numpy.count_nonzero(selection)

            if denom > 0.0:
                oldCluster = clusterCenters[clusterIndex].copy()
                clusterCenters[clusterIndex] = dataset[selection].sum(axis=0) / denom
                changes.append(numpy.sqrt(numpy.square(clusterCenters[clusterIndex] - oldCluster).sum()))

        allChangeSatisfied = all(x < allChangeThreshold for x in changes)
        halfChangeSatisfied = sum(x < halfChangeThreshold for x in changes) > clusterCenters.shape[0]/2.0

        if allChangeSatisfied and halfChangeSatisfied:
            break
    return bestClusterIndex, bestClusterDistance


def clusterQuality(clusterCenters, dataset):

    bestClusterDistance2 = None
    for clusterIndex in xrange(clusterCenters.shape[0]):
        distance2 = numpy.square(dataset - clusterCenters[clusterIndex,:]).sum(axis=1)
        if bestClusterDistance2 is None:
            bestClusterDistance2 = distance2
        else:
            better = (distance2 < bestClusterDistance2)
            bestClusterDistance2[better] = distance2[better]

    return numpy.sqrt(bestClusterDistance2.sum())

def mDist(imageArray,pcaEigenvalues):

    distance = numpy.sum(numpy.square(imageArray)/pcaEigenvalues,axis=1)

    return distance


def main(normalizedPixels,numberOfClusters):

    trials = []

    for i in xrange(10):

        # Initial clusterCenters are randomly selected from the dataset.
        clusterCenters = normalizedPixels[random.sample(xrange(normalizedPixels.shape[0]), numberOfClusters),:]
        # Perform k-means on a small subset of the pixels to quickly improve the initial seeds.
        kmeans(clusterCenters, normalizedPixels[random.sample(xrange(normalizedPixels.shape[0]),1000),:], numberOfIterations=10)
        # Rank the trials by quality.
        trials.append((clusterQuality(clusterCenters, normalizedPixels), clusterCenters.copy()))

        sys.stderr.write("completed %r iterations of k-means algorithm\n" % str(i))
        sys.stderr.write('reporter:status:still iterating a key\n')
        sys.stderr.flush()

    trials.sort()     # This could have used min(trials) instead; you don't need to sort them all...

    quality, clusterCenters = trials[0]

    # This is the real k-means run, using the whole dataset.
    clusterIndex, clusterDistance = kmeans(clusterCenters, normalizedPixels, numberOfIterations=10)

    return clusterCenters, clusterIndex, clusterDistance


if __name__=="__main__":

    metadata = None
    bands = {}
    ny = 0
    nx = 0

    parameterFile = open("analyticconfig")
    for line in parameterFile.readlines():
        line = line.rstrip().split('\t')
        if line[0]=="contourclusters.percentageOfAnomalyPixels":
            anomalyPercentage = float(line[1])
        elif line[0]=="contourclusters.numberOfClusters":
            numberOfClusters = int(line[1])
        elif line[0]=="contourclusters.histogramBinSize":
            binSize = int(line[1])

    for key,value in binaryhadoop.mapperInput(sys.stdin,typeMap={None: binaryhadoop.TYPEDBYTES_PICKLE}):
        regionKey = key
        for k,v in value.items():
            if k == "metadata":
                metadata = v
            elif k == "mask":
                mask = (v > 0)
                ny,nx = mask.shape
            else:
                bands[k] = v

    if metadata is not None:

        ny,nx = numpy.mgrid[0:ny,0:nx]
        ny = ny[mask]
        nx = nx[mask]

        imageArray = numpy.zeros((bands.values()[0].size,len(bands.keys())),dtype=numpy.float64)

        pcaEigenvalues = []
        for i,bandName in enumerate(sorted(bands.keys())):
            imageArray[:,i] = bands[bandName]
            pcaEigenvalues.append( metadata["principal_component_" + str(bandName).strip("band_")][0] )

        pcaEigenvalues = numpy.array(pcaEigenvalues).reshape(-1,1)       

        clusterCenters, clusterIndex, clusterDistance = main(imageArray,numberOfClusters)


        #clusterMahalanobisDistance is the mahalanobis distance of each cluster to the center of the space
        clusterMahalanobisDistance = list(mDist(clusterCenters,pcaEigenvalues.reshape(1,-1)))


        sys.stderr.write("completed k-means\n")
        clusterPopulations = [[x,list(clusterIndex).count(x)] for x in range(numberOfClusters)]

        clusterPopulations = numpy.array(clusterPopulations)[:,1]
        clusterPopulations = clusterPopulations[clusterIndex]
        clusterMDistance = numpy.array(clusterMahalanobisDistance)
        clusterMDistance = clusterMDistance[clusterIndex]

        sys.stderr.write("reporter:status:sorting clusters\n")

        clusterList = numpy.hstack((clusterIndex.reshape(-1,1),numpy.array(clusterPopulations).reshape(-1,1),clusterMDistance.reshape(-1,1),clusterDistance.reshape(-1,1),ny.reshape(-1,1),nx.reshape(-1,1)))
        clusterList = clusterList[numpy.lexsort((1./(1e-9 + clusterList[:,3]),1./(1e-9 + clusterList[:,2])))]

        scoreDict = {}
        for i in range(clusterList.shape[0]):
            scoreDict[tuple(map(int,numpy.around(clusterList[i,4:6])))] = [clusterList[i,2],clusterList[i,3]] 
      
        sys.stderr.write("ready to calculate cluster information\n")
        cluster = {}
        height,width = mask.shape
        for i,lbl in enumerate(set(clusterList[:round(anomalyPercentage*nx.size),0])):
            xedges = clusterList[clusterList[:,0]==lbl,5]
            yedges = clusterList[clusterList[:,0]==lbl,4]
            minClusterDistRowCol = list(clusterList[numpy.argsort(clusterList[clusterList[:,0]==lbl,3])[0],4:6])
            cluster["cluster_" + str(i)] = makeContours(xedges,yedges,width,height,binSize)
            cluster["cluster_" + str(i)].update({"clusterCenter": clusterCenters[lbl,:].tolist()})
            cluster["cluster_" + str(i)].update({"clusterCenterPixel": minClusterDistRowCol})


        sys.stderr.write("ready to emit cluster information\n")
        cluster["metadata"] = metadata


        #Assign contour scores
        for clusterKey, clusterValue in cluster.iteritems():
            if "metadata" not in clusterKey:
                for i in range(len(clusterValue['contours95'])):
                    sys.stderr.write('reporter:status:still computing score\n')
                    sys.stderr.flush()
                    polygons = numpy.array([[round(x[0]),round(x[1])] for x in clusterValue['contours95'][i]['rowcolpolygon']])
                    scoreIndex = []
                    for p in polygons:
                        scoreIndex.append(numpy.argmin(numpy.sum(numpy.square(numpy.fliplr(clusterList[:,4:6])-p),axis=1)))
                    scoremdist = mDist(numpy.array(cluster[clusterKey]["clusterCenter"]).reshape(1,-1),pcaEigenvalues.reshape(1,-1))[0] 
                    spectra = []
                    try:
                        scores = [scoreDict[x][1] for x in map(tuple,[map(int,map(round,y)) for y in clusterValue['contours95'][i]['rowcolpolygon']])]
                        scoreedist = sum(scores)/len(scores)
                    except KeyError:
                        scoreedist = numpy.mean(clusterList[numpy.array(scoreIndex),3])
                    cluster[clusterKey]['contours95'][i]['score'] = [scoremdist,scoreedist]
 
        binaryhadoop.emit(sys.stdout,regionKey,cluster,encoding = binaryhadoop.TYPEDBYTES_JSON)
