#!/usr/bin/env python

import sys
import time
try:
    import ConfigParser as configparser
except ImportError:
    import configparser

from utilities import *

if __name__ == "__main__":
    heartbeat = Heartbeat(stdout=False, stderr=True, reporter=True)
    heartbeat.write("%s Enter mapper-nestedKeys.py...\n" % time.strftime("%H:%M:%S"))

    config = configparser.ConfigParser()
    config.read(["../CONFIG.ini", "CONFIG.ini"])

    zoomDepthNarrowest = int(config.get("DEFAULT", "mapreduce.zoomDepthNarrowest"))
    zoomDepthWidest = int(config.get("DEFAULT", "mapreduce.zoomDepthWidest"))
    if zoomDepthWidest >= zoomDepthNarrowest:
        raise Exception("mapreduce.zoomDepthWidest must be a smaller number (lower zoom level) than mapreduce.zoomDepthNarrowest")

    heartbeat.write("%s Organizing T%02d tiles by their T%02d parents\n" % (time.strftime("%H:%M:%S"), zoomDepthNarrowest, zoomDepthWidest))
    for line in sys.stdin.xreadlines():
        depth, longIndex, latIndex, layer, timestamp = line.rstrip().split("-")
        parentDepth = int(depth[1:])
        parentLongIndex = int(longIndex)
        parentLatIndex = int(latIndex)

        while parentDepth > zoomDepthWidest:
            parentDepth, parentLongIndex, parentLatIndex = tileParent(parentDepth, parentLongIndex, parentLatIndex)

        parentKey = "%s-%s-%s" % (tileName(parentDepth, parentLongIndex, parentLatIndex), layer, timestamp)

        print "%s\t%s" % (parentKey, line.rstrip())

    heartbeat.write("%s Done.\n" % time.strftime("%H:%M:%S"))
