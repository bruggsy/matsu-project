#!/usr/bin/env python

import sys
import json

import numpy

import binaryhadoop
import utilities



def checkpca(pcaComponents, numberOfPcaComponents):

    loadVariancePercentile = [0.1,0.2]
    rogueBands = {}
    for i in xrange(pcaComponents.shape[0]):
        for j in xrange(pcaComponents.shape[1]):
            loadVariance = numpy.abs( numpy.std(pcaComponents[i,:]) - numpy.std( numpy.delete(pcaComponents[i,:],j) ) ) / numpy.std(pcaComponents[i,:])
            for l in loadVariancePercentile:
                if "loadVariance_" + str(l) not in rogueBands.keys():
                    rogueBands["loadVariance_" + str(l)] = []
                if loadVariance > l:             
                    rogueBands["loadVariance_" + str(l)].append([i,j,loadVariance])

    return rogueBands


def findpeaks(imagehist, w, x=None):

    peaks = []
    valleys = []

    if x is None:
        x = numpy.arange(len(imagehist))

    for i in numpy.arange(len(imagehist)):

        p = 1
        for j in numpy.arange( max(i-w,0), min(i+w,len(imagehist)) ):
            if imagehist[i] < imagehist[j]:
                p = -1
        if p==1:
            peaks.append([x[i],imagehist[i]])

    for i in numpy.arange(len(imagehist)):

        p = 1
        for j in numpy.arange( max(i-w,0), min(i+w,len(imagehist)) ):
            if imagehist[i] > imagehist[j]:
                p = -1
        if p==1:
            valleys.append([x[i],imagehist[i]])

    return peaks, valleys

def makeCovariance(imagelist, numpixels):

    idots = numpy.zeros((len(imagelist),len(imagelist))) 
    ipartials = numpy.zeros(len(imagelist)) 
    for i in xrange(len(imagelist)):
        ipartials[i] = imagelist[i].sum()
        for j in xrange(len(imagelist)):
            idots[i,j] = numpy.dot(imagelist[i],imagelist[j])


    A = 1./(numpixels - 1)
    B = A/numpixels

    covMatrix = A*idots - B*numpy.outer(ipartials,ipartials)

    return covMatrix


def main(imagelist):

    #find the covariance of the image bands
    sys.stderr.write("making covariance\n")
    imageCov = makeCovariance(imageList, numPixels)

    #find the principal components (eigenvectors/values)
    sys.stderr.write("making principle components 1\n")
    imgV, imgP = numpy.linalg.eig(imageCov)
    #
    sys.stderr.write("making principle components 2\n")
    indexList = numpy.argsort(-imgV)
    imgV = imgV[indexList]
    imgP = imgP[:,indexList]

    sys.stderr.write("making variance percentage\n")
    xVarianceComponents = 5
    variancePercentage = [x/numpy.sum(imgV) for x in imgV][:xVarianceComponents]

    sys.stderr.write("making rogue bands\n")
    rogueBands = checkpca(imgP.T,xVarianceComponents)

    sys.stderr.write("making gray bands\n")
    bandGray = numpy.zeros(len(imageList[0]))
    for band in imageList:
        bandGray += (numpy.array(band))**2
    bandGray = numpy.sort(bandGray)

    bandPercent = 1
    #The 99th percentile is removed to avoid skewing the mean
    bandGray = bandGray[bandGray < numpy.percentile(bandGray,100-bandPercent)]

    #Histogram is created 
    sys.stderr.write("making gray band histogram\n")
    [hist,bin_edges] = numpy.histogram(bandGray,bins=100)

    #Locate the peaks on the histogram
    sys.stderr.write("making peaks and valleys\n")
    peaks, valleys = findpeaks(hist,3,(bin_edges[:-1] + bin_edges[1:])/2)

    #Find mean and standard deviation of all pixels
    bandMean = numpy.mean(bandGray)
    bandSigma = numpy.std(bandGray)

    sys.stderr.write("making JSON output\n")
    imageData["numPixels"] = int(numPixels)

    imageData["grayBandMean"] = float(bandMean)
    imageData["grayBandSigma"] = float(bandSigma)

    #Report percentage of total pixels which lie beyond one standard deviation from mean
    imageData["grayBandPlusOneSigma"] = float(numpy.sum(bandGray > (bandMean+bandSigma))/numpy.float(numPixels))
    imageData["grayBandMinusOneSigma"] = float(numpy.sum(bandGray < (bandMean-bandSigma))/numpy.float(numPixels))

    imageData["grayBandHistPeaks"] = [[float(x), int(y)] for x, y in peaks]
    #imageData["grayBandHistValleys"] = [[float(x), int(y)] for x, y in valleys]

    #PCA analysis and sum of first 5 principal components
    imageData["grayBandExplainedVariance"] = [float(x) for x in variancePercentage]

    #Report bands that have high leave-one-out loading variance
    imageData["grayBandRogueBands"] = [str(x) for x in rogueBands]

    #Report histogram
    imageData["grayBandHistogram"] = [[float(x) for x in bin_edges], [int(x) for x in hist]]


    return imageData



if __name__=="__main__":

    imageData = {}
    imageData["metadata"] = None

    parameterFile = open("analyticconfig","r")
    for line in parameterFile.readlines():
        line = line.rstrip().split("\t")
        if line[0]=="contourclusters.noiseFlag":
            noiseFlag = line[1]
        #elif line[0]=="contourclusters.selectBands":
        #    selectBands = line[1]

    for key, value in binaryhadoop.mapperInput(sys.stdin):
        if key == "metadata":
            imageData["metadata"] = value
            bands = {}
            sys.stderr.write("    read metadata\n")
        elif key == "mask":
            mask = utilities.rollMask(value > 0)
            numPixels = numpy.nonzero(mask)[0].size 
            sys.stderr.write("    read mask\n")
        else:
            bands[key] = value[mask]
            sys.stderr.write("    read band %s\n" % key)

    if imageData["metadata"] is not None:
        if 'HSI' in imageData["metadata"].keys():
            wavelengths = {}
            multipliers = {}
            for w,wave in enumerate(imageData["metadata"][unicode("HSI")][unicode("wavelength")]):
                wavelengths["B" + "%03d" % w] = float(wave)
                multipliers["B" + "%03d" % w] = 1
        else:
            wavelengths = imageData["metadata"]["bandWavelength"]
            multipliers = imageData["metadata"]["bandMultiplier"]

        sys.stderr.write("making imageArray 1\n")
        imageList = utilities.preprocessImage(bands, multipliers, wavelengths, imageData)
        #imageList = utilities.preprocessImage(bands, multipliers, wavelengths, imageData, selectBands=selectbands)


        imageData["numPixels"] = numPixels
        main(imageList)

        #emit the final statistics
        sys.stderr.write("emiting summary statistics data\n")
        #storm.emit([localFileName, hdfsFileName, json.dumps(imageData)], stream="summaryStatistics")
        print json.dumps(imageData)


