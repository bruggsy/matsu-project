#!/usr/bin/env python

import math
import sys
import numpy


def makeGetLngLat(metadata):
    """Make a function that converts pixel indexes into longitude, latitude pairs."""

    try: 
        bl_lng, bl_lat = metadata["registration"]["bottom-left"]["lng"],   metadata["registration"]["bottom-left"]["lat"]
        bm_lng, bm_lat = metadata["registration"]["bottom-middle"]["lng"], metadata["registration"]["bottom-middle"]["lat"]
        br_lng, br_lat = metadata["registration"]["bottom-right"]["lng"],  metadata["registration"]["bottom-right"]["lat"]
        ml_lng, ml_lat = metadata["registration"]["middle-left"]["lng"],   metadata["registration"]["middle-left"]["lat"]
        mm_lng, mm_lat = metadata["registration"]["middle-middle"]["lng"], metadata["registration"]["middle-middle"]["lat"]
        mr_lng, mr_lat = metadata["registration"]["middle-right"]["lng"],  metadata["registration"]["middle-right"]["lat"]
        tl_lng, tl_lat = metadata["registration"]["top-left"]["lng"],      metadata["registration"]["top-left"]["lat"]
        tm_lng, tm_lat = metadata["registration"]["top-middle"]["lng"],    metadata["registration"]["top-middle"]["lat"]
        tr_lng, tr_lat = metadata["registration"]["top-right"]["lng"],     metadata["registration"]["top-right"]["lat"]
    except KeyError:
        bl_lng, bl_lat = metadata["geotag"]["registration"]["bottom-left"]["lng"],   metadata["geotag"]["registration"]["bottom-left"]["lat"]
        bm_lng, bm_lat = metadata["geotag"]["registration"]["bottom-middle"]["lng"], metadata["geotag"]["registration"]["bottom-middle"]["lat"]
        br_lng, br_lat = metadata["geotag"]["registration"]["bottom-right"]["lng"],  metadata["geotag"]["registration"]["bottom-right"]["lat"]
        ml_lng, ml_lat = metadata["geotag"]["registration"]["middle-left"]["lng"],   metadata["geotag"]["registration"]["middle-left"]["lat"]
        mm_lng, mm_lat = metadata["geotag"]["registration"]["middle-middle"]["lng"], metadata["geotag"]["registration"]["middle-middle"]["lat"]
        mr_lng, mr_lat = metadata["geotag"]["registration"]["middle-right"]["lng"],  metadata["geotag"]["registration"]["middle-right"]["lat"]
        tl_lng, tl_lat = metadata["geotag"]["registration"]["top-left"]["lng"],      metadata["geotag"]["registration"]["top-left"]["lat"]
        tm_lng, tm_lat = metadata["geotag"]["registration"]["top-middle"]["lng"],    metadata["geotag"]["registration"]["top-middle"]["lat"]
        tr_lng, tr_lat = metadata["geotag"]["registration"]["top-right"]["lng"],     metadata["geotag"]["registration"]["top-right"]["lat"]

    try:
        width = float(metadata["width"])
    except KeyError:
        width = float(metadata["geotag"]["width"])
    try:
        height = float(metadata["height"])
    except KeyError:
        height = float(metadata["geotag"]["height"])

    dlngdx = (mr_lng - ml_lng) / width
    dlatdy = (tm_lat - bm_lat) / height
    dlatdx = (mr_lat - ml_lat) / width
    dlngdy = (tm_lng - bm_lng) / height
    d2latdy2 = 2.0 * (tm_lat - 2.0*mm_lat + bm_lat) / (height)**2
    d2lngdx2 = 2.0 * (mr_lng - 2.0*mm_lng + ml_lng) / (width)**2
    d2lngdy2 = 2.0 * (tm_lng - 2.0*mm_lng + bm_lng) / (height)**2
    d2latdx2 = 2.0 * (mr_lat - 2.0*mm_lat + ml_lat) / (width)**2
    d2latdxdy = (tr_lat - br_lat - tl_lat + bl_lat) / (height * width)
    d2lngdxdy = (tr_lng - br_lng - tl_lng + bl_lng) / (height * width)

    def getLngLat(pixelx, pixely):
        x = (pixelx - width/2.0)
        y = (pixely - height/2.0)
        return mm_lng + dlngdx*x + dlngdy*y + d2lngdx2*x**2 + d2lngdy2*y**2 + d2lngdxdy*x*y, \
               mm_lat + dlatdy*y + dlatdx*x + d2latdy2*y**2 + d2latdx2*x**2 + d2latdxdy*x*y

    return getLngLat


def makeGetMeters(metadata):
    """Make a function that converts longitude, latitude pairs into metersEast, metersNorth pairs.

    This transformation is based on the standard WGS'84 oblate spheroid Earth model.
    """
    try:
        mm_lng, mm_lat = metadata["registration"]["middle-middle"]["lng"], metadata["registration"]["middle-middle"]["lat"]
    except KeyError:
        mm_lng, mm_lat = metadata["geotag"]["registration"]["middle-middle"]["lng"], metadata["geotag"]["registration"]["middle-middle"]["lat"]    

    A = 6378.1370
    B = 6356.7523142
    ECCENTRICITY2 = (A*A - B*B)/A/A
    CENTER_LON = mm_lng
    CENTER_LAT = mm_lat
    LON_TO_KM = (math.pi*A) * math.cos(CENTER_LAT*math.pi/180.0) / 180.0 / math.sqrt(1.0 - ECCENTRICITY2*math.pow(math.sin(CENTER_LAT*math.pi/180.0), 2))
    LAT_TO_KM = (math.pi*A) * (1.0 - ECCENTRICITY2) / 180.0 / math.pow(1.0 - ECCENTRICITY2*math.pow(math.sin(CENTER_LAT*math.pi/180.0), 2), 1.5)

    def getMeters(lng, lat):
        return (lng - CENTER_LON) * LON_TO_KM * 1000.0, (lat - CENTER_LAT) * LAT_TO_KM * 1000.0

    return getMeters

def incidentSolar(wavelength):
    """Incident solar radiance in a given wavelength, unnormalized.
    The normalization constant is just to return a reasonable order of
    magnitude (0-100) for wavelengths in the few hundred nm range
    (visible and near-IR)."""

    return 2.5e-28/(math.exp(0.0143878/(wavelength*1e-9)/(5778)) - 1.)/(wavelength*1e-9)**5


def rollMask(mask):
    for iteration in (1,2):
        maskLeft = numpy.roll(mask, 1, axis=0)
        maskRight = numpy.roll(mask, -1, axis=0)
        maskUp = numpy.roll(mask, 1, axis=1)
        maskDown = numpy.roll(mask, -1, axis=1)

        numpy.logical_and(mask, maskLeft, mask)
        numpy.logical_and(mask, maskRight, mask)
        numpy.logical_and(mask, maskUp, mask)
        numpy.logical_and(mask, maskDown, mask)

    return mask


def fixMask(mask,bands):

    for band in bands:
        for i in numpy.arange(mask.shape[0]):
            if numpy.std(bands[band][i,:]) < 1e-6:
                mask[i,:] = numpy.zeros((mask.shape[1]))
        for j in numpy.arange(mask.shape[1]):
            if numpy.std(bands[band][:,j]) < 1e-6:
                mask[:,j] = numpy.zeros((mask.shape[0]))

    return mask


def preprocessImageGeneralBands(bands, multipliers, wavelengths, imageData):
    """Called once per image to normalize bands by solar radiance and apply a transformation."""

    if len(bands) == 0: return

    imageList = []
    imageData["bandNames"] = []
    for bandName, bandImage in sorted(bands.items(),key = lambda x: x[0]):
        if numpy.std(bandImage) > 1e-6:
            numpy.multiply(bandImage, (multipliers[bandName] / incidentSolar(wavelengths[bandName])), bandImage)
            #numpy.log(bandImage + 1e-6,bandImage)   #avoid taking log of 0  
            imageList.append(bandImage)
            imageData["bandNames"].append(bandName)

    bands = {}
    return imageList

def preprocessImage(bands, multipliers, wavelengths, imageData, selectBands=True):
    """Called once per image to normalize bands by solar radiance and apply a transformation."""

    #These bands correspond to "good" Hyperion bands [S. Pederson; 06/10/2013]
    #The default is to keep them and remove others, however this can be overriden with the directive False
    #when calling utilities.preprocessImage(.....,False)
    bandsToKeep =   ['B010','B011','B012','B013','B014','B017','B018','B019','B020','B021','B022',
                     'B023','B024','B025','B026','B027','B028','B029','B030','B031','B032','B033',
                     'B034','B035','B036','B037','B038','B040','B042','B043','B044','B045','B046',
                     'B048','B049','B050','B051','B052','B053','B054','B055','B056','B057','B082',
                     'B083','B084','B085','B086','B087','B088','B090','B092','B093','B094','B095',
                     'B096','B102','B103','B104','B105','B106','B107','B108','B109','B110','B111',
                     'B112','B113','B114','B115','B116','B117','B118','B119','B120','B135','B136',
                     'B137','B138','B139','B140','B141','B142','B143','B144','B146','B147','B148',
                     'B149','B150','B151','B152','B153','B154','B155','B156','B157','B158','B159'
                     'B160','B161','B162','B163']

    if len(bands) == 0: return

    if selectBands == False:
        bandsToKeep = bands.keys()

    #Need to remove bands from initial image that are not common to the image and the wavelengths/multipliers dictionaries.
    commonBands = set(bands.keys()) & set(wavelengths.keys()) & set(bandsToKeep)

    for bandName in bands.keys():
        if bandName not in commonBands:
            del bands[bandName]

    imageList = []
    imageData["bandNames"] = []

    for bandName, bandImage in sorted(bands.items(),key = lambda x: x[0]):
        if numpy.sum(bandImage)==0:
            del bands[bandName]
            sys.stderr.write("skipping band %r because it is all zero\n" % bandName)
        elif numpy.std(bandImage) > 1e-6:
            numpy.multiply(bandImage, (multipliers[bandName] / incidentSolar(wavelengths[bandName])), bandImage)
            numpy.log(bandImage + 1e-6,bandImage)
            imageList.append(bandImage)
            imageData["bandNames"].append(bandName)
        elif numpy.std(bandImage) < 1e-6:
            del bands[bandName]
            sys.stderr.write("skipping band because it contains no variation\n")
        else:
            del bands[bandName]

    return imageList


def contours(hist2d, xedges, yedges, threshold, interpolate=True, smooth=True):
    """Convert raster data into vector data by tracing polygon contours at a threshold value.

    hist2d, xedges, yedges are the output of the numpy.histogram2d function:

        numpy.histogram2d(xpositions, ypositions, bins=(numXBins, numYBins), range=[[xlow, xhigh], [ylow, yhigh]])

    It can also be constructed by hand:

        hist2d = numpy.zeros((numXbins, numYbins))    # and then fill it
        xedges = numpy.linspace(xlow, xhigh, numXbins + 1)
        yedges = numpy.linspace(ylow, yhigh, numYbins + 1)

    The threshold is the desired cut through the z value of the 2d
    histogram; it should be between hist2d.min() and hist2d.max().

    Different thresholds can lead to different numbers of polygons:
    think of it as the sea level around an archipelago/mountain
    range--- high sea levels can turn peninsulas into islands into
    nothing.

    If interpolate=True (the default), use the relative heights of
    nearby bins to improve the contour line positions.  This
    improvement introduces data from the original hist2d that is not
    in the threshold-cut histogram.

    If smooth=True (the default), apply a LOESS smoothing to the
    resulting polygons with a Gaussian weight function (sigma = step
    size).  Smoothed polygons have five times as many edges to
    approximate a smoother curve.  This improvement does not introduce
    data; it only makes the curves easier to interpret visually.

    The output of this function is a list of polygons, where each
    polygon is a list of (x, y) tuples.  You may want to call this
    function several times with different thresholds to make a
    bulls-eye.
    """

    cuthist2d = (hist2d > threshold)

    xwidth = (xedges[-1] - xedges[0]) / (len(xedges) - 1.0)
    ywidth = (yedges[-1] - yedges[0]) / (len(yedges) - 1.0)
    xhalfwidth = xwidth / 2.0
    yhalfwidth = ywidth / 2.0

    up = "up"
    down = "down"
    left = "left"
    right = "right"

    def gethist(hist, i, j):
        if 0 <= i < hist.shape[0] and 0 <= j < hist.shape[1]:
            return hist[i,j]
        elif hist.dtype == numpy.bool:
            return False
        else:
            return 0.0

    if interpolate:
        def step(i, j, nextStep, polygon):
            previousStep = nextStep

            state = 0
            if not ((i-1) < 0 or (j-1) < 0 or (i-1) >= cuthist2d.shape[0] or (j-1) >= cuthist2d.shape[1]) and gethist(cuthist2d, (i-1), (j-1)):
                state |= 1
            if not ((i) < 0 or (j-1) < 0 or (i) >= cuthist2d.shape[0] or (j-1) >= cuthist2d.shape[1]) and gethist(cuthist2d, (i), (j-1)):
                state |= 2
            if not ((i-1) < 0 or (j) < 0 or (i-1) >= cuthist2d.shape[0] or (j) >= cuthist2d.shape[1]) and gethist(cuthist2d, (i-1), (j)):
                state |= 4
            if not ((i) < 0 or (j) < 0 or (i) >= cuthist2d.shape[0] or (j) >= cuthist2d.shape[1]) and gethist(cuthist2d, (i), (j)):
                state |= 8

            nextStep = None
            if state == 0b0001:
                nextStep = up
                polygon.append((xedges[i] + (threshold - gethist(hist2d, i-1,j-1))/(gethist(hist2d, i,j-1) - gethist(hist2d, i-1,j-1))*xwidth - xhalfwidth, yedges[j] - yhalfwidth))
            elif state == 0b0010:
                nextStep = right
                polygon.append((xedges[i] + xhalfwidth, yedges[j] + (threshold - gethist(hist2d, i,j-1))/(gethist(hist2d, i,j) - gethist(hist2d, i,j-1))*ywidth - yhalfwidth))
            elif state == 0b0011:
                nextStep = right
                polygon.append((xedges[i] + xhalfwidth, yedges[j] + (threshold - gethist(hist2d, i,j-1))/(gethist(hist2d, i,j) - gethist(hist2d, i,j-1))*ywidth - yhalfwidth))
            elif state == 0b0100:
                nextStep = left
                polygon.append((xedges[i] - xhalfwidth, yedges[j] + (threshold - gethist(hist2d, i-1,j-1))/(gethist(hist2d, i-1,j) - gethist(hist2d, i-1,j-1))*ywidth - yhalfwidth))
            elif state == 0b0101:
                nextStep = up
                polygon.append((xedges[i] + (threshold - gethist(hist2d, i-1,j-1))/(gethist(hist2d, i,j-1) - gethist(hist2d, i-1,j-1))*xwidth - xhalfwidth, yedges[j] - yhalfwidth))
            elif state == 0b0110:
                if previousStep is up:
                    nextStep = left
                    polygon.append((xedges[i] + (threshold - gethist(hist2d, i-1,j))/(gethist(hist2d, i,j) - gethist(hist2d, i-1,j))*xwidth - xhalfwidth, yedges[j] + yhalfwidth))
                    polygon.append((xedges[i] + xhalfwidth, yedges[j] + (threshold - gethist(hist2d, i,j-1))/(gethist(hist2d, i,j) - gethist(hist2d, i,j-1))*ywidth - yhalfwidth))
                else:
                    nextStep = right
                    polygon.append((xedges[i] + (threshold - gethist(hist2d, i-1,j-1))/(gethist(hist2d, i,j-1) - gethist(hist2d, i-1,j-1))*xwidth - xhalfwidth, yedges[j] - yhalfwidth))
                    polygon.append((xedges[i] - xhalfwidth, yedges[j] + (threshold - gethist(hist2d, i-1,j-1))/(gethist(hist2d, i-1,j) - gethist(hist2d, i-1,j-1))*ywidth - yhalfwidth))
            elif state == 0b0111:
                nextStep = right
                polygon.append((xedges[i] + xhalfwidth, yedges[j] + (threshold - gethist(hist2d, i,j-1))/(gethist(hist2d, i,j) - gethist(hist2d, i,j-1))*ywidth - yhalfwidth))
            elif state == 0b1000:
                nextStep = down
                polygon.append((xedges[i] + (threshold - gethist(hist2d, i-1,j))/(gethist(hist2d, i,j) - gethist(hist2d, i-1,j))*xwidth - xhalfwidth, yedges[j] + yhalfwidth))
            elif state == 0b1001:
                if previousStep is right:
                    nextStep = up
                    polygon.append((xedges[i] + xhalfwidth, yedges[j] + (threshold - gethist(hist2d, i,j-1))/(gethist(hist2d, i,j) - gethist(hist2d, i,j-1))*ywidth - yhalfwidth))
                    polygon.append((xedges[i] + (threshold - gethist(hist2d, i-1,j-1))/(gethist(hist2d, i,j-1) - gethist(hist2d, i-1,j-1))*xwidth - xhalfwidth, yedges[j] - yhalfwidth))
                else:
                    nextStep = down
                    polygon.append((xedges[i] - xhalfwidth, yedges[j] + (threshold - gethist(hist2d, i-1,j-1))/(gethist(hist2d, i-1,j) - gethist(hist2d, i-1,j-1))*ywidth - yhalfwidth))
                    polygon.append((xedges[i] + (threshold - gethist(hist2d, i-1,j))/(gethist(hist2d, i,j) - gethist(hist2d, i-1,j))*xwidth - xhalfwidth, yedges[j] + yhalfwidth))
            elif state == 0b1010:
                nextStep = down
                polygon.append((xedges[i] + (threshold - gethist(hist2d, i-1,j))/(gethist(hist2d, i,j) - gethist(hist2d, i-1,j))*xwidth - xhalfwidth, yedges[j] + yhalfwidth))
            elif state == 0b1011:
                nextStep = down
                polygon.append((xedges[i] + (threshold - gethist(hist2d, i-1,j))/(gethist(hist2d, i,j) - gethist(hist2d, i-1,j))*xwidth - xhalfwidth, yedges[j] + yhalfwidth))
            elif state == 0b1100:
                nextStep = left
                polygon.append((xedges[i] - xhalfwidth, yedges[j] + (threshold - gethist(hist2d, i-1,j-1))/(gethist(hist2d, i-1,j) - gethist(hist2d, i-1,j-1))*ywidth - yhalfwidth))
            elif state == 0b1101:
                nextStep = up
                polygon.append((xedges[i] + (threshold - gethist(hist2d, i-1,j-1))/(gethist(hist2d, i,j-1) - gethist(hist2d, i-1,j-1))*xwidth - xhalfwidth, yedges[j] - yhalfwidth))
            elif state == 0b1110:
                nextStep = left
                polygon.append((xedges[i] - xhalfwidth, yedges[j] + (threshold - gethist(hist2d, i-1,j-1))/(gethist(hist2d, i-1,j) - gethist(hist2d, i-1,j-1))*ywidth - yhalfwidth))

            return nextStep

    else:
        def step(i, j, nextStep, polygon):
            previousStep = nextStep

            state = 0
            if not ((i-1) < 0 or (j-1) < 0 or (i-1) >= cuthist2d.shape[0] or (j-1) >= cuthist2d.shape[1]) and gethist(cuthist2d, (i-1), (j-1)):
                state |= 1
            if not ((i) < 0 or (j-1) < 0 or (i) >= cuthist2d.shape[0] or (j-1) >= cuthist2d.shape[1]) and gethist(cuthist2d, (i), (j-1)):
                state |= 2
            if not ((i-1) < 0 or (j) < 0 or (i-1) >= cuthist2d.shape[0] or (j) >= cuthist2d.shape[1]) and gethist(cuthist2d, (i-1), (j)):
                state |= 4
            if not ((i) < 0 or (j) < 0 or (i) >= cuthist2d.shape[0] or (j) >= cuthist2d.shape[1]) and gethist(cuthist2d, (i), (j)):
                state |= 8

            nextStep = None
            if state == 0b0001:
                nextStep = up
            elif state == 0b0010:
                nextStep = right
            elif state == 0b0011:
                nextStep = right
            elif state == 0b0100:
                nextStep = left
            elif state == 0b0101:
                nextStep = up
            elif state == 0b0110:
                if previousStep is up:
                    nextStep = left
                else:
                    nextStep = right
            elif state == 0b0111:
                nextStep = right
            elif state == 0b1000:
                nextStep = down
            elif state == 0b1001:
                if previousStep is right:
                    nextStep = up
                else:
                    nextStep = down
            elif state == 0b1010:
                nextStep = down
            elif state == 0b1011:
                nextStep = down
            elif state == 0b1100:
                nextStep = left
            elif state == 0b1101:
                nextStep = up
            elif state == 0b1110:
                nextStep = left

            if state != 0:
                polygon.append((xedges[i], yedges[j]))

            return nextStep

    covered = set()
    polygons = []

    i, j = 0, 0
    while i < len(xedges) - 2:
        try:
            while i < len(xedges) - 2:
                onThisLine = numpy.nonzero(cuthist2d[i])[0]
                for j in onThisLine:
                    if (i, j) not in covered:
                        raise StopIteration
                i += 1
        except StopIteration:
            pass
        else:
            break

        if i >= len(xedges) - 2: break

        polygon = []

        starti, startj = i, j
        nextStep = None
        while True:
            covered.add((i, j))
            nextStep = step(i, j, nextStep, polygon)

            if nextStep is up:
                j -= 1
            elif nextStep is left:
                i -= 1
            elif nextStep is down:
                j += 1
            elif nextStep is right:
                i += 1
            else:
                break

            if i == starti and j == startj:
                break

        i, j = starti, startj
        if len(polygon) >= 3:
            polygons.append(polygon)

    if smooth:
        smoothpolygons = []
        for polygon in polygons:
            smoothpolygon = []
            for i in xrange(len(polygon)):
                iprevprev = (i - 2) % len(polygon)
                iprev = (i - 1) % len(polygon)
                inext = (i + 1) % len(polygon)
                inextnext = (i + 2) % len(polygon)
                for newi in [i + x/float(5) for x in xrange(5)]:
                    sum1 = 0.0
                    sumi = 0.0
                    sumii = 0.0
                    sumix = 0.0
                    sumiy = 0.0
                    sumx = 0.0
                    sumy = 0.0
                    for eye, (x, y), weight in (i - 2, polygon[iprevprev], 0.05), \
                                               (i - 1, polygon[iprev], 0.25), \
                                               (i, polygon[i], 0.4), \
                                               (i + 1, polygon[inext], 0.25), \
                                               (i + 2, polygon[inextnext], 0.05):
                        sum1 += weight
                        sumi += weight * eye
                        sumii += weight * eye**2
                        sumix += weight * eye * x
                        sumiy += weight * eye * y
                        sumx += weight * x
                        sumy += weight * y
                    delta = (sum1 * sumii) - (sumi * sumi)
                    interceptx = ((sumii * sumx) - (sumi * sumix)) / delta
                    slopex = ((sum1 * sumix) - (sumi * sumx)) / delta
                    intercepty = ((sumii * sumy) - (sumi * sumiy)) / delta
                    slopey = ((sum1 * sumiy) - (sumi * sumy)) / delta
                    newx = interceptx + (newi * slopex)
                    newy = intercepty + (newi * slopey)
                smoothpolygon.append((newx, newy))
            smoothpolygons.append(smoothpolygon)
        return smoothpolygons

    else:
        return polygons


def cutLevel(hist2d, percentile):
    return numpy.percentile(hist2d[hist2d > 0], percentile)


def circumference(polygon):
    total = 0.0
    for i in xrange(len(polygon)):
        x1, y1 = polygon[i]
        x2, y2 = polygon[(i + 1) % len(polygon)]
        total += math.sqrt((x1 - x2)**2 + (y1 - y2)**2)
    return total


def area(polygon):
    total = 0.0
    for i in xrange(len(polygon)):
        x1, y1 = polygon[i]
        x2, y2 = polygon[(i + 1) % len(polygon)]
        total += (x1*y2 - x2*y1)
    return 0.5*total

def centroid(polygon):
    Cx = 0.0
    Cy = 0.0
    A = area(polygon)
    for i in xrange(len(polygon)):
        x1, y1 = polygon[i]
        x2, y2 = polygon[(i + 1) % len(polygon)]
        Cx += (x1 + x2)*(x1*y2 - x2*y1)
        Cy += (y1 + y2)*(x1*y2 - x2*y1)
    return Cx/(6*A), Cy/(6*A)

def convert(polygon, getLngLat):
    newPolygon = []
    for x, y in polygon:
        newPolygon.append(getLngLat(x, y))
    return newPolygon

def getRegionKey(metadata):
     if unicode("outputFile") in metadata.keys():
         regionKey = str(metadata[unicode("outputFile")])
     else:
         regionKey = [v for n, v in metadata["L1T"]["PRODUCT_METADATA"].items() if n[-10:] == "_FILE_NAME"][0][:22]
     return regionKey

