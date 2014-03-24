#!/usr/bin/env python

import sys
import json

import binaryhadoop

def main():

    print json.dumps(pcaData)

if __name__ == "__main__":


    pcaData = {}
    region_key = None
    for pca in sys.stdin:

        thisPCA = json.loads(pca)
        region_key = thisPCA["region_key"]

        del thisPCA["region_key"]

        if region_key is not None:
            pcaData[region_key] = thisPCA

    main()
