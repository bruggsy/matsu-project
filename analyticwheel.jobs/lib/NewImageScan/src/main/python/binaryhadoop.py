#!/usr/bin/env python

"""This module does all of the SequenceFile-PNG-Numpy conversions that do not require external libraries (other than Numpy)."""

import os
import glob
import struct
import zlib
import base64
import subprocess
import json
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO
try:
    import cPickle as pickle
except ImportError:
    import pickle

import numpy

if "HADOOP_HOME" in os.environ:
    HADOOP_EXECUTABLE = os.path.join(os.environ["HADOOP_HOME"], "bin", "hadoop")
    try:
        HADOOP_STREAMING_JAR = sorted(glob.glob(os.path.join(os.environ["HADOOP_HOME"], "contrib", "streaming", "hadoop-streaming-*.jar")))[-1]
    except IndexError:
        HADOOP_STREAMING_JAR = None
else:
    HADOOP_EXECUTABLE = None
    HADOOP_STREAMING_JAR = None

if HADOOP_EXECUTABLE is None:
    HADOOP_EXECUTABLE = "/usr/bin/hadoop"
if HADOOP_STREAMING_JAR is None:
    try:
        HADOOP_STREAMING_JAR = glob.glob("/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh*.jar")[0]
    except IndexError:
        HADOOP_STREAMING_JAR = "/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.3.0.jar"

PNG_HEADER = "\211PNG\r\n\032\n"

# typedbytes protocol is given on http://hadoop.apache.org/docs/current/api/org/apache/hadoop/typedbytes/package-summary.html
# application-defined typecodes (I made these up; 50-200 can be user-specified)
TYPEDBYTES_RAW = 0
TYPEDBYTES_PNG = 50
TYPEDBYTES_JSON = 51
TYPEDBYTES_PICKLE = 52
TYPEDBYTES_ARRAY = 53

PICKLE_PROTOCOL = 2

def hadoopCall(stdin, *args):
    process = subprocess.Popen([HADOOP_EXECUTABLE] + list(args), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate(stdin)
    return process.returncode, stdout, stderr

def arrayToPng(array, base64encode=False):
    file = StringIO.StringIO()
    
    def writeChunk(tag, data):
        file.write(struct.pack("!I", len(data)))
        file.write(tag)
        file.write(data)
        cyclicRedundancyCheck = zlib.crc32(tag)
        cyclicRedundancyCheck = zlib.crc32(data, cyclicRedundancyCheck)
        cyclicRedundancyCheck &= 0xffffffff
        file.write(struct.pack("!I", cyclicRedundancyCheck))

    height, width = array.shape

    if array.dtype == ">u1":
        scanlineLength = width + 1
        bitdepth = 8
    elif array.dtype == ">u2":
        scanlineLength = 2 * width + 1
        array = array.view(">u1")
        bitdepth = 16
    else:
        raise ValueError("array dtype is %r" % array.dtype)

    scanlines = numpy.empty((height, scanlineLength), dtype=">u1")
    scanlines[:,0] = 0   # first bit of each line is a filter that we don't use
    scanlines[:,1:] = array
    scanlines = numpy.reshape(scanlines, height * scanlineLength)

    file.write(PNG_HEADER)
    writeChunk("IHDR", struct.pack("!2I5B", width, height, bitdepth, 0, 0, 0, 0))
    writeChunk("IDAT", zlib.compress(str(scanlines.data)))
    writeChunk("IEND", "")

    if base64encode:
        return base64.b64encode(file.getvalue())
    else:
        return file.getvalue()

def pngToArray(png, base64decode=False):
    if base64decode:
        file = StringIO.StringIO(base64.b64decode(png))
    else:
        file = StringIO.StringIO(png)

    def readChunk():
        length = struct.unpack("!I", file.read(4))
        tag = file.read(4)
        data = file.read(length[0])
        crcExpected = zlib.crc32(tag)
        crcExpected = zlib.crc32(data, crcExpected)
        crcExpected &= 0xffffffff
        crcObserved = struct.unpack("!I", file.read(4))
        if crcObserved[0] != crcExpected:
            raise IOError("cyclic redundancy check in tag \"%s\" does not match" % tag)
        return tag, data

    if file.read(len(PNG_HEADER)) != PNG_HEADER:
        raise IOError("Not a valid PNG file (is it base64 encoded?)")

    tag, data = readChunk()
    if tag != "IHDR":
        raise IOError("First chunk must be \"IHDR\", not \"%s\"" % tag)

    width, height, bitdepth, colorType, compressionMethod, filterMethod, interlaceMethod = struct.unpack("!2I5B", data)
    if width <= 0 or height <= 0:
        raise IOErorr("Non-positive width or height")
    if bitdepth != 8 and bitdepth != 16:
        raise NotImplementedError("Only bit depths of 8 or 16 are supported")
    if colorType != 0:
        raise NotImplementedError("Only greyscale PNG files are supported")
    if compressionMethod != 0 or filterMethod != 0 or interlaceMethod != 0:
        raise NotImplementedError("Non-zlib compression, filtering, and interlacing are all not supported")

    if bitdepth == 8:
        dtype = ">u1"
        scanlineLength = width + 1
    elif bitdepth == 16:
        dtype = ">u2"
        scanlineLength = 2 * width + 1

    tag, data = readChunk()
    if tag != "IDAT":
        raise NotImplementedError("Second chunk must be \"IDAT\", not \"%s\"" % tag)

    scanlines = numpy.reshape(numpy.fromstring(zlib.decompress(data), dtype=">u1"), (height, scanlineLength))
    array = numpy.reshape(numpy.reshape(scanlines[:,1:], height * (scanlineLength - 1)).view(dtype), (height, width))

    tag, data = readChunk()
    if tag != "IEND":
        raise NotImplementedError("Third (and final) chunk must be \"IEND\", not \"%s\"" % tag)

    return array

def seqPNGToHDFS(outputFile, metadata, bandDataGenerator, sorterLength=0):
    process = subprocess.Popen([HADOOP_EXECUTABLE, "jar", HADOOP_STREAMING_JAR, "loadtb", outputFile], stdin=subprocess.PIPE)

    # SequenceFile key
    process.stdin.write(struct.pack("!bi", 0, len("metadata") + 5))
    process.stdin.write(struct.pack("!bi", 0, len("metadata")))
    process.stdin.write("metadata")
    # SequenceFile value
    serializedMetadata = json.dumps(metadata)
    process.stdin.write(struct.pack("!bi", TYPEDBYTES_JSON, len(serializedMetadata) + sorterLength + 5))
    process.stdin.write(struct.pack("!bi", TYPEDBYTES_JSON, len(serializedMetadata) + sorterLength))
    process.stdin.write("\0" * sorterLength)
    process.stdin.write(serializedMetadata)

    for bandName in metadata["bandNames"]:
        data = bandDataGenerator(bandName)

        process.stdin.write(struct.pack("!bi", 0, len(bandName) + 5))
        process.stdin.write(struct.pack("!bi", 0, len(bandName)))
        process.stdin.write(bandName)
        process.stdin.write(struct.pack("!bi", TYPEDBYTES_PNG, len(data) + sorterLength + 5))
        process.stdin.write(struct.pack("!bi", TYPEDBYTES_PNG, len(data) + sorterLength))
        process.stdin.write("\0" * sorterLength)
        process.stdin.write(data)

    process.stdin.close()

    returncode = process.wait()
    if returncode != 0:
        raise IOError("Could not create SequenceFile from typedbytes")

def writeToHDFS(outputFile, keys, values, sorterLength=0):
    process = subprocess.Popen([HADOOP_EXECUTABLE, "jar", HADOOP_STREAMING_JAR, "loadtb", outputFile], stdin=subprocess.PIPE)

    for key, value in zip(keys, values):
        process.stdin.write(struct.pack("!bi", 0, len(key) + 5))
        process.stdin.write(struct.pack("!bi", 0, len(key)))
        process.stdin.write(key)

        if isinstance(value, basestring):
            serializedValue = value
            typecode = TYPEDBYTES_RAW

        elif isinstance(value, dict):
            serializedValue = json.dumps(serializedValue)
            typecode = TYPEDBYTES_JSON

        elif isinstance(value, numpy.ndarray):
            if len(value.shape) == 2:
                serializedValue = arrayToPng(value, base64encode=False)
                typecode = TYPEDBYTES_PNG

            else:
                buff = StringIO.StringIO()
                numpy.save(buff, value)
                serializedValue = buff.getvalue()
                typecode = TYPEDBYTES_ARRAY

        else:
            serializedValue = pickle.dumps(serializedValue)
            typecode = TYPEDBYTES_PICKLE

        process.stdin.write(struct.pack("!bi", typecode, len(serializedValue) + sorterLength + 5))
        process.stdin.write(struct.pack("!bi", typecode, len(serializedValue) + sorterLength))
        process.stdin.write("\0" * sorterLength)
        process.stdin.write(serializedValue)

    process.stdin.close()

    returncode = process.wait()
    if returncode != 0:
        raise IOError("Could not create SequenceFile from typedbytes")

def readFromHDFS(inputFile, sorterLength=0):
    data = []
    for key, sorter, interpretedValue in readFromHDFSiter(inputFile, sorterLength):
        data.append((key, sorter, interpretedValue))
    return data

def readFromHDFSiter(inputFile, sorterLength=0):
    process = subprocess.Popen([HADOOP_EXECUTABLE, "jar", HADOOP_STREAMING_JAR, "dumptb", inputFile], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # stdout, stderr = process.communicate()
    # if process.returncode != 0:
    #     raise IOError("Could not read SequenceFile into typedbytes: %s" % stderr)

    metadata = {"bandNames": [], "base64encoded": False}

    # index = 0
    # while index < len(stdout):
    while True:
        # index += 5
        if not process.stdout.read(5): break

        # header = stdout[index:(index + 5)]
        # index += 5
        header = process.stdout.read(5)
        if not header: break

        typecode, length = struct.unpack("!bi", header)
        if typecode != 0:
            raise NotImplementedError("SequenceFile key typecode is %d, rather than 0 (for binary)" % typecode)

        # key = stdout[index:(index + length)]
        # index += length
        key = process.stdout.read(length)
        if not key: break

        # index += 5
        if not process.stdout.read(5): break

        # header = stdout[index:(index + 5)]
        # index += 5
        header = process.stdout.read(5)
        if not header: break

        typecode, length = struct.unpack("!bi", header)

        # value = stdout[index:(index + length)]
        # index += length
        value = process.stdout.read(length)
        if not value: break

        sorter = value[:sorterLength]
        value = value[sorterLength:]

        if typecode == TYPEDBYTES_RAW:
            interpretedValue = value

        elif typecode == TYPEDBYTES_PNG:
            interpretedValue = pngToArray(value, base64decode=metadata["base64encoded"])

        elif typecode == TYPEDBYTES_JSON:
            interpretedValue = json.loads(value)

        elif typecode == TYPEDBYTES_PICKLE:
            interpretedValue = pickle.loads(value)

        elif typecode == TYPEDBYTES_ARRAY:
            buff = StringIO.StringIO(value)
            interpretedValue = numpy.load(buff)

        else:
            raise NotImplementedError("Unrecognized typecode: %d" % typecode)

        if key == "metadata":
            metadata = interpretedValue

        yield (key, sorter, interpretedValue)

def emit(stream, key, value, sorter="", sorterLength=0, encoding=TYPEDBYTES_PICKLE, **pngOptions):
    if encoding == TYPEDBYTES_RAW:
        serialized = value

    elif encoding == TYPEDBYTES_PNG:
        serialized = arrayToPng(value, **pngOptions)

    elif encoding == TYPEDBYTES_JSON:
        serialized = json.dumps(value)

    elif encoding == TYPEDBYTES_PICKLE:
        serialized = pickle.dumps(value, protocol=PICKLE_PROTOCOL)

    elif encoding == TYPEDBYTES_ARRAY:
        buff = StringIO.StringIO()
        numpy.save(buff, value)
        serialized = buff.getvalue()

    else:
        raise NotImplementedError("Unrecognized typecode: %d" % typecode)

    stream.write(struct.pack("!bi", 0, len(key) + 5))
    stream.write(struct.pack("!bi", 0, len(key)))
    stream.write(key)
    
    stream.write(struct.pack("!bi", encoding, len(serialized) + sorterLength + 5))
    stream.write(struct.pack("!bi", encoding, len(serialized) + sorterLength))

    if len(sorter) > sorterLength:
        sorter = sorter[:sorterLength]
    elif len(sorter) < sorterLength:
        sorter = sorter + "\0" * (sorterLength - len(sorter))
    stream.write(sorter)

    stream.write(serialized)

def mapperInput(stream, sorterLength=0, typeMap={None: TYPEDBYTES_PNG, "metadata": TYPEDBYTES_JSON}, **pngOptions):
    while True:
        keyheader = stream.read(4)
        if not keyheader:
            break

        length = struct.unpack("!i", keyheader)[0]
        key = stream.read(length)[10:]

        tab = stream.read(1)
        if tab != "\t":
            raise IOError("SequenceFiles are served to mappers as binary-tab-binary-eoln, but we encountered %r-%r-%r-..." % (keyheader, key, tab))

        length = struct.unpack("!i", stream.read(4))[0]
        value = stream.read(length)[10:]

        sorter = value[:sorterLength]
        value = value[sorterLength:]

        eoln = stream.read(1)
        if eoln != "\n":
            raise IOError("SequenceFiles are served to mappers as binary-tab-binary-eoln, but we encountered %r-%r-%r-(%d bytes)-%r..." % (keyheader, key, tab, length, eoln))

        typecode = typeMap.get(key, typeMap.get(None, TYPEDBYTES_RAW))

        if typecode == TYPEDBYTES_RAW:
            interpretedValue = value

        elif typecode == TYPEDBYTES_PNG:
            interpretedValue = pngToArray(value, **pngOptions)

        elif typecode == TYPEDBYTES_JSON:
            interpretedValue = json.loads(value)

        elif typecode == TYPEDBYTES_PICKLE:
            interpretedValue = pickle.loads(value)

        elif typecode == TYPEDBYTES_ARRAY:
            buff = StringIO.StringIO(value)
            interpretedValue = numpy.load(buff)

        else:
            raise NotImplementedError("Unrecognized typecode: %d" % typecode)

        yield key, interpretedValue

def reducerInput(stream, sorterLength=0, **pngOptions):
    while True:
        keyheader = stream.read(5)
        if len(keyheader) < 5:
            break

        typecode, length = struct.unpack("!bi", keyheader)
        key = stream.read(length)[5:]

        typecode, length = struct.unpack("!bi", stream.read(5))
        value = stream.read(length)[5:]

        sorter = value[:sorterLength]
        value = value[sorterLength:]

        if typecode == TYPEDBYTES_RAW:
            interpretedValue = value

        elif typecode == TYPEDBYTES_PNG:
            interpretedValue = pngToArray(value, **pngOptions)

        elif typecode == TYPEDBYTES_JSON:
            interpretedValue = json.loads(value)

        elif typecode == TYPEDBYTES_PICKLE:
            interpretedValue = pickle.loads(value)

        elif typecode == TYPEDBYTES_ARRAY:
            buff = StringIO.StringIO(value)
            interpretedValue = numpy.load(buff)

        else:
            raise NotImplementedError("Unrecognized typecode: %d" % typecode)

        yield key, sorter, interpretedValue
