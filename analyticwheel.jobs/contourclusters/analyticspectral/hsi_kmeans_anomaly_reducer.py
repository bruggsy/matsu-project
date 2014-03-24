#!/usr/bin/env python

import sys
import json

import binaryhadoop

if __name__=="__main__":

    
    for key, sorter, value in binaryhadoop.reducerInput(sys.stdin):
        print '%s\t%s' % (key,json.dumps(value))



