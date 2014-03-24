#!/usr/bin/env python

import sys
import json
import numpy

import binaryhadoop

def findCommonBands(bandListPic,bandListRef):
    bandsCommon = list(set(bandListPic) & set(bandListRef))
    bandsCommon = sorted(bandsCommon)
    return bandsCommon 


def removeUncommonBandInfo(commonBands,iband,idot,ipartial):
    dropBandsNum = []
    for band in iband:
        if band not in commonBands:
            dropBandsNum.append(iband.index(band))
    idot = numpy.array(idot)
    idot = numpy.delete(idot,dropBandsNum,axis=0)
    idot = numpy.delete(idot,dropBandsNum,axis=1)
    ipartial = numpy.array(ipartial)
    ipartial = numpy.delete(ipartial,dropBandsNum)
 
    return idot.tolist(),ipartial.tolist()


def makeCovariance(ibands,idots,ipartials,numpixels):

    #convert unicode band names in list to ascii
    for i in xrange(len(ibands)):
        ibands[i] = sorted([str(x) for x in ibands[i]])

    #find the greatest common set of bands
    commonBands = ibands[0]
    test1 = commonBands
    for bands in ibands:
        commonBands = findCommonBands(bands,commonBands)
    test2 = commonBands
    sys.stderr.write("Test 1 equals test 2: %r\n" % bool(1-(bool(set(test1)-set(test2)) ))   )


    for i in xrange(len(ibands)):
        idots[i],ipartials[i] = removeUncommonBandInfo(commonBands,ibands[i],idots[i],ipartials[i]) 

    #all lists now have same dimensions, so convert to numpy arrays for easy algebraic manipulation
    idots = numpy.array(idots)         #This is a 3-dimensional array: number of images X number of bands X number of bands
    ipartials = numpy.array(ipartials) #This is a 2-dimensional array: number of images X number of bands

    #These are the coefficients for the two terms of the covariance matrix
    A = 1./(numpixels-1)
    B = A/numpixels
    covMatrix = A*idots.sum(axis=0) - B*numpy.outer(ipartials.sum(axis=0),ipartials.sum(axis=0))

    return covMatrix, commonBands


def main(region_key,ibands,numpixels,idots,ipartials,ndots,npartials,metadata):

    #Eigenvector decomps sometimes lead to small
    #machine precision complex numbers
    #assume very small numbers are 0
    numpy.set_printoptions(suppress=True)

    #initialize principal components dict
    pca_dict = {}

    covarianceMatrix, commonBands = makeCovariance(ibands,idots,ipartials,numpixels)
    sys.stderr.write("This is the size of the covariance matrix: (%r,%r)\n" % covarianceMatrix.shape)


    if len(ndots) != 0:
        sortOrder = 1

        noiseCovarianceMatrix, commonBands = makeCovariance(ibands,ndots,npartials,numpixels)
        covarianceMatrix = numpy.dot(numpy.linalg.inv(covarianceMatrix),noiseCovarianceMatrix)

        wG, vG = numpy.linalg.eig(covarianceMatrix)

    else:
        sortOrder = -1

        wG, vG = numpy.linalg.eig(covarianceMatrix)

    indexList = numpy.argsort(sortOrder*wG)           #sort according to standard pca ordering or reverse for noise
    wG = wG[indexList].tolist()
    vG = vG[:,indexList].T.tolist()


    for i in xrange(len(wG)):  
        pca_dict["principal_component_" + str(i+1)] = [wG[i],vG[i]] 
   
    sys.stderr.write("This is the number of pca bands %r\n" % len(commonBands)) 
    #add the identities of bands used in constructing the pca
    metadata["pca_from_bands"] = commonBands  
    #add the metadata to the pca dict
    pca_dict["metadata"] = metadata
    #include region key
    pca_dict["region_key"] = region_key

    print json.dumps(pca_dict)

if __name__ == "__main__":
    '''
    all data from a region is directed to the same reducer
    the data is of the form REGION_N SEPARATOR image_data.json
    load the data from a region into the appropriate list
    '''

    last_key = []
    for key, sorter, value in binaryhadoop.reducerInput(sys.stdin):

        #initialized last_key to key first time through
        #unload image data and initialize all lists


        if last_key == []:
            last_key = key
            imageData = value
            iBands = [imageData["bandNames"]]
            iDots = [imageData["imageDot"]]
            iPartials = [imageData["imagePartial"]]
            try:
                nDots = [imageData["noiseDot"]]
                nPartials = [imageData["noisePartial"]]
            except:
                nDots = []
                nPartials = []
            numPixels = imageData["numPixels"]
            metadata = imageData["metadata"] 

        elif last_key != key:
            #if there is a new key, i.e., region, pass all of the data currently in the region dictionary to the reducer
            main(last_key,iBands,numPixels,iDots,iPartials,nDots,nPartials,metadata)
            last_key = key
            #load image data and re-initialize lists
            imageData = value
            iBands = [imageData["bandNames"]]
            iDots = [imageData["imageDot"]]
            iPartials = [imageData["imagePartial"]]
            try:
                nDots = [imageData["noiseDot"]]
                nPartials = [imageData["noisePartial"]]
            except:
                nDots = []
                nPartials = []
            numPixels = imageData["numPixels"]
            metadata = imageData["metadata"]  
            
        else:
            #if key is the same as last key, append image data to lists
            imageData = value
            iBands.append(imageData["bandNames"])
            iDots.append(imageData["imageDot"])
            iPartials.append(imageData["imagePartial"])
            try:
                nDots.append(imageData["noiseDot"])
                nPartials.append(imageData["noisePartial"])
            except:
                pass
            numPixels += imageData["numPixels"]
            metadata.update(imageData["metadata"])

    #call main a final time to get the image data from the final key
    main(last_key,iBands,numPixels,iDots,iPartials,nDots,nPartials,metadata)
