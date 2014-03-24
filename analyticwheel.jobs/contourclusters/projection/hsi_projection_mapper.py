#!/usr/bin/env python

import sys
import json
import pickle
import numpy

import binaryhadoop
import utilities


def removeBands(dataArray,removalBands):

    return numpy.delete(dataArray,removalBands,axis=0)

def checkPCA(pcaComponents,numberofPcaComponents):

    rogueBands = []
    for i in xrange(numberOfPcaComponents,pcaComponents.shape[0]):
        for j in xrange(pcaComponents.shape[1]):
            loadVariance = numpy.abs( numpy.std(pcaComponents[i,:]) - numpy.std( numpy.delete(pcaComponents[i,:],j) ) ) / numpy.std(pcaComponents[i,:]) 
            if loadVariance > 0.8:              #If the variance loading ratio just appended is greater than 0.8
                rogueBands.append([i,j,loadVariance])

    return rogueBands 


def pcaProcessImage(imageArray,pcaData,imageBands):
    '''
    Do the projection of the image onto the pca bands.  First figure out
    which bands were used to construct the pca.   
    '''

    for pcaKeys in pcaData["metadata"]:
        if pcaKeys == "pca_from_bands":
            pcaFromBands = pcaData["metadata"]["pca_from_bands"]


    sys.stderr.write("This is the number of image bands: %r\n" % len(imageBands))
    sys.stderr.write("This is the number of pca bands: %r\n" % len(pcaFromBands))

    commonBandsSet = set(pcaFromBands) & set(imageBands)    

    remove_from_pca = list(set(pcaFromBands) - commonBandsSet)
    remove_from_pca_nums = numpy.array([pcaFromBands.index(x) for x in sorted(remove_from_pca)])

    remove_from_image = list(set(imageBands) - commonBandsSet)
    remove_from_image_nums = numpy.array([imageBands.index(x) for x in sorted(remove_from_image)])

    imageArray = numpy.array(imageArray)
    imageArray = numpy.delete(imageArray,remove_from_image_nums,axis=0)

    imageMean = numpy.mean(imageArray,axis=1)
    imageArray = imageArray - imageMean.reshape(-1,1)

    pcaEigenvalues = []
    pcaComponents = []
    for key,value in sorted(pcaData.items(),key=lambda x: x[0]):
        if key != "metadata":
            pcaComponents.append(value[1])

    pcaComponents = numpy.array(pcaComponents,dtype=numpy.float64)  #The rows of this matrix are the pca vectors
    pcaComponents = numpy.delete(pcaComponents,numpy.array(remove_from_pca_nums),axis=0)
    pcaComponents = numpy.delete(pcaComponents,numpy.array(remove_from_pca_nums),axis=1)
    sys.stderr.write("This is the size of the pcaComponents: (%r,%r)\n" % pcaComponents.shape)
    sys.stderr.write("This is the size of the image array: (%r,%r)\n" % imageArray.shape)

    virtualBands = numpy.dot(pcaComponents,imageArray) 

    return virtualBands, pcaComponents, imageMean


if __name__ == "__main__":

    pcaData = json.load(open("part-00000","r"))

    parameterFile = open("analyticconfig","r")
    for line in parameterFile.readlines():
        line = line.rstrip().split('\t')
        if line[0]=="contourclusters.numberOfPcaComponents":
            numberOfPcaComponents = int(line[1])
        elif line[0]=="contourclusters.selectBands":
            selectbands = line[1]

    metadata = None
    bands = {}
    for key, value in binaryhadoop.mapperInput(sys.stdin):
        if key == "metadata":
            metadata = value
            bands = {}
        elif key == "mask":
            mask = utilities.rollMask(value > 0)
        else:
            bands[key] = numpy.array(value, dtype=numpy.float64)

    sys.stderr.write("This is the number of bands before pre-processing: %r\n" % len(bands))
    if metadata is not None:

        mask = utilities.fixMask(mask,bands)
        for bandKey, bandValue in bands.iteritems():
            bands[bandKey] = bandValue[mask]

        if 'HSI' in metadata.keys():
            wavelengths = {}
            multipliers = {}
            for w,wave in enumerate(metadata["HSI"]["wavelength"]):
                wavelengths["B" + "%03d" % w] = float(wave)
                multipliers["B" + "%03d" % w] = 1
        else:
            wavelengths = metadata["bandWavelength"]
            multipliers = metadata["bandMultiplier"]
        
        try:
            regionKey = metadata["originalDirName"]
        except KeyError:
            regionKey = metadata["outputFile"]

        pca_data = pcaData[regionKey]
        for i in xrange(numberOfPcaComponents):
            metadata["principal_component_" + str(i+1)] = pca_data["principal_component_" + str(i+1)]


        imageList = utilities.preprocessImage(bands,multipliers,wavelengths,{},selectBands=selectbands)
        sys.stderr.write("This is the number of bands after pre-processing: %r\n" % len(imageList))
        virtualBands, pcaComponents, imageMean = pcaProcessImage(imageList,pca_data,sorted(bands.keys()))
        rogueBands = checkPCA(pcaComponents,numberOfPcaComponents)
        metadata["image mean"] = imageMean.tolist()   
 
        if len(rogueBands) > 0:
            sys.stderr.write("These are the dropped bands that have high pca loading variance: \n")
            for rogue in rogueBands:
                sys.stderr.write("[PCA component, Load index, Leave-one-out variance] " + str(rogue) + "\n")
        else:
            sys.stderr.write("There were no dropped bands that have high pca loading variance\n") 
        
        projectedImage = {}
        projectedImage["metadata"] = metadata
        projectedImage["mask"] = mask
        for i in xrange(numberOfPcaComponents):
            projectedImage["band_" + str(i+1)] = virtualBands[i,:]
       
         
        binaryhadoop.emit(sys.stdout,regionKey,projectedImage, encoding = binaryhadoop.TYPEDBYTES_PICKLE)
