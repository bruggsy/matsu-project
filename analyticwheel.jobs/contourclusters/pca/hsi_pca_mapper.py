#!/usr/bin/env python

import sys
import json


import numpy

import binaryhadoop
import utilities


def calcNoise(imageArray,mask):
    ny,nx = numpy.nonzero(mask)
    rotatedMaskRows = []
    for y in list(set(list(ny)))[10:-10]:
        rotatedMaskRows.append(numpy.where(ny==y)[0])

    noiseArray = []
    for band in imageArray:
        noise = []
        for row in rotatedMaskRows:
            noise +=  list(band[row[1:]] - band[row[:-1]])
        noiseArray.append(numpy.array(noise))
    return noiseArray


def main(imageArray):
    iDot = []
    iPartial = []
    for imageRow in imageArray:
        iPartial.append(imageRow.sum())
        iRow = []
        for imageCol in imageArray:
            iRow.append(numpy.dot(imageRow,imageCol))
        iDot.append(iRow)
    return iDot, iPartial


if __name__ == "__main__":

    imageData = {}
    imageData["metadata"] = None

    parameterFile = open("analyticconfig","r")
    for line in parameterFile.readlines():
        line = line.rstrip().split("\t")
        if line[0]=="contourclusters.noiseFlag":
            noiseFlag = line[1]
        elif line[0]=="contourclusters.selectBands":
            selectbands = line[1]

    for key, value in binaryhadoop.mapperInput(sys.stdin):
        if key == "metadata":
            imageData["metadata"] = value 
            bands = {}
        elif key == "mask":
            mask = utilities.rollMask(value > 0)
            imageData["numPixels"] = numpy.nonzero(mask)[0].size
        else:
            bands[key] = numpy.array(value,dtype=numpy.float64)

    if imageData["metadata"] is not None:

        mask = utilities.fixMask(mask,bands)
        for bandKey, bandValue in bands.iteritems():
            bands[bandKey] = bandValue[mask]


        if 'HSI' in imageData["metadata"].keys():
            wavelengths = {}
            multipliers = {}
            for w,wave in enumerate(imageData["metadata"][unicode("HSI")][unicode("wavelength")]):
                wavelengths["B" + "%03d" % w] = float(wave)
                multipliers["B" + "%03d" % w] = 1
        else:
            wavelengths = imageData["metadata"]["bandWavelength"]
            multipliers = imageData["metadata"]["bandMultiplier"]        
        
        imageList = utilities.preprocessImage(bands, multipliers, wavelengths, imageData, selectBands=selectbands)
        sys.stderr.write("This is the number of bands: %r\n" % len(imageList))

        iDot,iPartial = main(imageList)
        imageData["imageDot"] = iDot
        imageData["imagePartial"] = iPartial

        if noiseFlag.upper() == "TRUE":
            noiseList = calcNoise(imageList,mask)
            nDot,nPartial = main(noiseList)
            imageData["noiseDot"] = nDot
            imageData["noisePartial"] = nPartial
        try:
            regionKey = imageData["metadata"]["originalDirName"]
        except KeyError:
            regionKey = imageData["metadata"]["outputFile"]

        binaryhadoop.emit(sys.stdout,regionKey,imageData,encoding=binaryhadoop.TYPEDBYTES_JSON)
