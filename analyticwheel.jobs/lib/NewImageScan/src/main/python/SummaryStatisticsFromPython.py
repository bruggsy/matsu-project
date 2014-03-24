#!/usr/bin/env python

import json

import numpy

import binaryhadoop
import utilities
import storm


class SummaryStatisticsBolt(storm.BasicBolt):
    def initialize(self, stormconf, context):
        binaryhadoop.HADOOP_EXECUTABLE = "/opt/hadoop/bin/hadoop"
        binaryhadoop.HADOOP_STREAMING_JAR = "/opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar"

    def checkpca(self, pcaComponents, numberOfPcaComponents):

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


    def findpeaks(self, imagehist, w, x=None):

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

    def makeCovariance(self, imagelist, numpixels):

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


    def process(self, tup):

        localFileName = tup.values[0]
        hdfsFileName = tup.values[1]
        imageData = {}
        imageData["metadata"] = None

        storm.log("start processing %s %s" % (localFileName, hdfsFileName))

        for key, sorter, interpretedValue in binaryhadoop.readFromHDFSiter(hdfsFileName):
            if key == "metadata":
                imageData["metadata"] = interpretedValue
                bands = {}
                storm.log("    read metadata")
            elif key == "mask":
                mask = utilities.rollMask(interpretedValue > 0)
                numPixels = numpy.nonzero(mask)[0].size 
                storm.log("    read mask")
            else:
                bands[key] = interpretedValue[mask]
                storm.log("    read band %s" % key)

        if imageData["metadata"] is not None:
            wavelengths = imageData["metadata"]["bandWavelength"]
            multipliers = imageData["metadata"]["bandMultiplier"]

            storm.log("making imageArray 1")
            imageList = utilities.preprocessImage(bands, multipliers, wavelengths, imageData)
        
            #find the covariance of the image bands
            storm.log("making covariance")
            imageCov = self.makeCovariance(imageList, numPixels)  

            #find the principal components (eigenvectors/values)
            storm.log("making principle components 1")
            imgV, imgP = numpy.linalg.eig(imageCov)
            #
            storm.log("making principle components 2")
            indexList = numpy.argsort(-imgV)
            imgV = imgV[indexList]
            imgP = imgP[:,indexList]

            storm.log("making variance percentage")
            xVarianceComponents = 5
            variancePercentage = [x/numpy.sum(imgV) for x in imgV][:xVarianceComponents]

            storm.log("making rogue bands")
            rogueBands = self.checkpca(imgP.T,xVarianceComponents)

            storm.log("making gray bands")
            bandGray = numpy.zeros(len(imageList[0]))
            for band in imageList:
                bandGray += (numpy.array(band))**2
            bandGray = numpy.sort(bandGray) 

            bandPercent = 1
            #The 99th percentile is removed to avoid skewing the mean
            bandGray = bandGray[bandGray < numpy.percentile(bandGray,100-bandPercent)] 
 
            #Histogram is created 
            storm.log("making gray band histogram")
            [hist,bin_edges] = numpy.histogram(bandGray,bins=100)

            #Locate the peaks on the histogram
            storm.log("making peaks and valleys")
            peaks, valleys = self.findpeaks(hist,3,(bin_edges[:-1] + bin_edges[1:])/2) 

            #Find mean and standard deviation of all pixels
            bandMean = numpy.mean(bandGray)
            bandSigma = numpy.std(bandGray)
        
            storm.log("making JSON output")
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

            #emit the final statistics
            storm.log("emiting Storm tuple")
            storm.emit([localFileName, hdfsFileName, json.dumps(imageData)], stream="summaryStatistics")

            storm.log("done with %s %s" % (localFileName, hdfsFileName))

SummaryStatisticsBolt().run()
