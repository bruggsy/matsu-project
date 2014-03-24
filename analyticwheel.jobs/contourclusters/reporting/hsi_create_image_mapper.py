#!/usr/bin/env python

import sys
import json
import numpy

import binaryhadoop
import utilities

import rasterAndPolygonsToSvg as makeSvg

from augustus.plot  import addPlotting
from augustus.strict import *
from augustus.producer.PlotToolkit import PlotToolkit
addPlotting(modelLoader)
p=PlotToolkit(modelLoader)

def plotSpectrum(spectrum, wavelengths):
    dataDictionary = modelLoader.loadXml("""
    <PMML version="4.1" xmlns="http://www.dmg.org/PMML-4_1">
      <Header/>
      <DataDictionary>
          <DataField name="Spectrum" dataType="double" optype="continuous"/>
          <DataField name="Wavelength" dataType="double" optype="continuous"/>
      </DataDictionary>
    </PMML>
    """)
    centerspectrum = {'Spectrum':spectrum, 'Wavelength':wavelengths}
    dataTable=dataDictionary.calc(centerspectrum)
    #plot = p.scatter('Wavelength','Spectrum')
    plot = p.parametricPointsCurve('Wavelength','Spectrum', xlabel="wavelength (nm)", ylabel="relative reflextivity")
   
    return plot.calculate(dataTable).xml()
  

if __name__ == "__main__":

    #the clustercontours file contains key tab json
    #of the results from the contour clustering algorithm
    contour_data = open('clustercontours','r')
    contour_dict = {}
    for line in contour_data.readlines():
        key = line.rstrip().split('\t')[0]
        value = line.rstrip().split('\t')[1]
        contour_dict[key] = json.loads(value)

    parameterFile = open("analyticconfig")
    for line in parameterFile.readlines():
        line = line.rstrip().split('\t')
        if line[0]=="contourclusters.selectBands":
            val = line[1]
            if val.rstrip() in ['True', 'TRUE', 'true', '1', 't', 'y', 'yes']:
                selectbands = True
            else:
                selectbands = False

    metadata = None
    ny = 0
    nx = 0

    for key, value in binaryhadoop.mapperInput(sys.stdin):
        if key == "metadata":
            metadata = value
            try:
                thisImageKey = metadata['originalDirName']
            except KeyError:
                thisImageKey = metadata['outputFile']
            bands = {}
        elif key == "mask":
            mask = utilities.rollMask(value > 0)
            numberOfRows,numberOfColumns = mask.shape
        else:
            bands[key] = numpy.array(value[mask], dtype=numpy.float64)

    if metadata is not None:
        if 'HSI' in metadata.keys():
            wavelengths = {}
            multipliers = {}
            desiredBandsToUse = ['B004', 'B008', 'B021']
            for w,wave in enumerate(metadata["HSI"]["wavelength"]):
                wavelengths["B" + "%03d" % w] = float(wave)
                multipliers["B" + "%03d" % w] = 1
        else:
            wavelengths = metadata["bandWavelength"]
            multipliers = metadata["bandMultiplier"]
            desiredBandsToUse = ['B013', 'B020', 'B042']


        ny,nx = numpy.mgrid[0:numberOfRows,0:numberOfColumns]
        ny = ny[mask]
        nx = nx[mask]


        imageList = utilities.preprocessImage(bands, multipliers, wavelengths, {},selectBands=selectbands)

        sys.stderr.write("Requested bands are: %r\n" % desiredBandsToUse)
        rgbdict = {}
        bandKeyNums = [int(x.strip('B')) for x in bands.keys()]
        for i,band in enumerate(desiredBandsToUse):
            bandNum = int(band.strip('B'))
            b = min((abs(bandNum - i), i) for i in bandKeyNums)[1]
            sys.stderr.write("Will use band  %s\n" % str(b))
            rgbdict[i] = 'B' + '%03d' % (b)
            bandKeyNums.remove(b)
        sys.stderr.write("Closest bands to requested are: %r\n" % rgbdict)

        # Make sure we have three bands (RGB):
        # This shouldn't every happen, but I'm putting it in for safety.
        if len(rgbdict) == 0:
            sys.stderr.write("No usable RGB bands, exiting")
            raise ValueError("No usable RGB bands")
        elif len(rgbdict) == 1:
            sys.stderr.write("Missing two bands, creating a monochrome image")
            bandsToUse = 3*[rgbdict.values()[0]]
        elif len(rgbdict) == 2:
            sys.stderr.write("Missing on band, duplicating one band for the image")
            # Which ever band is missing will be replaced with the first one.
            bandstoUse = 3*[rgbdict.values()[0]]
            for k in rgbdict.keys():
                bandsToUse[k] = rgbdict[k]
        else:
            bandsToUse = [ rgbdict[0], rgbdict[1], rgbdict[2] ]


        sys.stderr.write("Using as RGB: %r\n" % bandsToUse)
        newwavelengths = [wavelengths[x] for x in sorted(list( set(wavelengths.keys()) & set(bands.keys()) ))]

        imageArray1 = numpy.array(imageList)
        numpy.exp(imageArray1,imageArray1)
        imageArray = numpy.array(imageList)[tuple([sorted(bands.keys()).index(x) for x in bandsToUse]),:]
        numpy.exp(imageArray,imageArray)

        imageRGB = numpy.zeros(mask.shape + (3,))

        rgbMin = imageArray.min()
        rgbMax = imageArray.max()


        colorList = ["#ff0087","#00ff78","#7700ff","#ffb2f4","#ffff00","#00ff00","#ff00ff","#ff0000","#ff6633","#66ff00"]

        for i in numpy.arange(nx.size):
            imageRGB[ny[i],nx[i],0] = 255.*(imageArray[0,i] - rgbMin)/(rgbMax - rgbMin)
            imageRGB[ny[i],nx[i],1] = 255.*(imageArray[1,i] - rgbMin)/(rgbMax - rgbMin)
            imageRGB[ny[i],nx[i],2] = 255.*(imageArray[2,i] - rgbMin)/(rgbMax - rgbMin)


        replacementPixels = []
        clusterSpectra = {}
        colorCounter = 0
        nyx = map(tuple,numpy.hstack((ny.reshape(-1,1),nx.reshape(-1,1))).tolist())
        for clusterKey, clusterValues in contour_dict[thisImageKey].iteritems():
            if 'metadata' not in clusterKey:
                clusterSpectra[clusterKey] = ""
                if len(clusterValues["contours95"]) > 0:
                    sys.stderr.write("This is the image index: %r\n" % nyx.index(tuple(clusterValues["clusterCenterPixel"])))
                    clusterspectrum = list(imageArray1[:,nyx.index(tuple(clusterValues["clusterCenterPixel"]))]) 
                    clusterSpectra[clusterKey] = plotSpectrum(clusterspectrum,newwavelengths)
                    for i in xrange(len(clusterValues["contours95"])):
                        replacementPixels.append({"style": "stroke: " + colorList[colorCounter % len(colorList)],"points": clusterValues["contours95"][i]["rowcolpolygon"], "cluster": clusterKey})
            colorCounter += 1

        imageRGB = numpy.array(imageRGB,dtype=numpy.uint8)
        imageSvg = makeSvg.rasterAndPolygonsToSvg(imageRGB,mask,replacementPixels)
        print '%s\t%s' % (thisImageKey,json.dumps({"contoured image": imageSvg.split('\n')[2], "replacement pixels": replacementPixels, "cluster spectra": clusterSpectra}))
