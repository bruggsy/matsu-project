import struct
import zlib
import base64
import StringIO
import sys

import numpy
from lxml.etree import ElementTree
from lxml.builder import ElementMaker

SVG_NAMESPACE = "http://www.w3.org/2000/svg"
SVG_FILE_HEADER = """<?xml version="1.0" standalone="no"?>
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">
"""
XLINK_NAMESPACE = "http://www.w3.org/1999/xlink"
XLINK_HREF = "{%s}href" % XLINK_NAMESPACE

def rasterToPng(array, alpha):
    output = StringIO.StringIO()

    # red, green, blue, alpha are assumed to be flat, uint8 Numpy arrays of the same length
    height, width = alpha.shape
    interleaved = numpy.empty(4 * width * height, dtype=numpy.uint8)
    interleaved[0::4] = numpy.reshape(array[:,:,2], (-1,))
    interleaved[1::4] = numpy.reshape(array[:,:,1], (-1,))
    interleaved[2::4] = numpy.reshape(array[:,:,0], (-1,))
    interleaved[3::4] = numpy.reshape(alpha, (-1,))
    interleaved = numpy.reshape(interleaved, (height, 4 * width))

    scanlines = numpy.empty((height, 4 * width + 1), dtype=numpy.uint8)
    scanlines[:,0] = 0  # first byte of each scanline is zero
    scanlines[:,1:] = interleaved
    scanlines = numpy.reshape(scanlines, height * (4 * width + 1))

    def writeChunk(tag, data):
        output.write(struct.pack("!I", len(data)))
        output.write(tag)
        output.write(data)
        cyclicRedundancyCheck = zlib.crc32(tag)
        cyclicRedundancyCheck = zlib.crc32(data, cyclicRedundancyCheck)
        cyclicRedundancyCheck &= 0xffffffff
        output.write(struct.pack("!I", cyclicRedundancyCheck))

    output.write("\211PNG\r\n\032\n")
    writeChunk("IHDR", struct.pack("!2I5B", width, height, 8, 6, 0, 0, 0))
    writeChunk("IDAT", zlib.compress(str(scanlines.data)))
    writeChunk("IEND", "")

    return base64.b64encode(output.getvalue())

def normalizeRaster(red, green, blue):
    # I'm not sure if you'll need this or if you'll be supplying your
    # own.  The 5%-95% percentile method works better than this
    # 0%-100% min-max method, but I'm in a hurry.
    minimum = float(min(red.min(), green.min(), blue.min()))
    maximum = float(max(red.max(), green.max(), blue.max()))

    return numpy.array(255 * (red - minimum)/(maximum - minimum), dtype=numpy.uint8), \
           numpy.array(255 * (green - minimum)/(maximum - minimum), dtype=numpy.uint8), \
           numpy.array(255 * (blue - minimum)/(maximum - minimum), dtype=numpy.uint8)

def normalizeMaskToAlpha(mask):
    # Masks are typically binary (0 or 1) whereas an alpha channel
    # varies from 0 to 255.  This assumes that you have a mask and
    # gives you a simple alpha.
    return numpy.array(255 * (mask > 0), dtype=numpy.uint8)

def rasterAndPolygonsToSvg(imagearray, mask, polygons): #image array is assumed to be height x width x 3; blue:0, green:1, red:2
    E = ElementMaker(namespace=SVG_NAMESPACE, nsmap={None: SVG_NAMESPACE, "xlink": XLINK_NAMESPACE})

    height, width = mask.shape
    wholeImage = E.svg(version="1.1", \
                       width="100%", \
                       height="100%", \
                       preserveAspectRatio="xMidYMin meet", \
                       viewBox="0 0 %d %d" % (width, height), \
                       style="fill: none; stroke: black; stroke-linejoin: miter; stroke-width: 2; text-anchor: middle;")

    # Based on how your Numpy arrays are structured, you may want a
    # different normalization scheme (see comments above).
    #sys.stderr.write("This is the size of the arrays: (%r,%r),(%r,%r)\n" % (imagearray.shape,mask.shape))
    rasterAsBase64Png = rasterToPng(imagearray, normalizeMaskToAlpha(mask))

    rasterElement = E.image(**{XLINK_HREF: "data:image/png;base64," + rasterAsBase64Png, \
                               "x": "0", "y": "0", "width": repr(width), "height": repr(height), \
                               "preserveAspectRatio": "none"})
    wholeImage.append(rasterElement)

    # Convert the polygons from a dictionary into a bunch of SVG paths
    # and add them to the image.
    for polygon in polygons:
        points = ["L %r %r" % (x, y) for x, y in polygon["points"]]
        if len(points) > 0:
            points[0]= "M" + points[0][1:]   # first is a moveto (M), not a lineto (L)
            points.append("Z")               # close the polygon
            polygonElement = E.path(d=" ".join(points), style=polygon["style"])
            wholeImage.append(polygonElement)
                
    # If you're writing to disk, you can lower the overhead by writing
    # directly to a file object, rather than a StringIO.
    output = StringIO.StringIO()
    output.write(SVG_FILE_HEADER)
    ElementTree(wholeImage).write(output)
    return output.getvalue()
