package com.opendatagroup.hyperspectral.seqpng;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import java.util.zip.Deflater;
import java.util.zip.CRC32;

public class ImageBandArray {
    public final byte[] PNG_HEADER = {-119, 80, 78, 71, 13, 10, 26, 10};
    public final byte[] IHDR = {73, 72, 68, 82};
    public final byte[] IDAT = {73, 68, 65, 84};
    public final byte[] IEND = {73, 69, 78, 68};

    public final int width;
    public final int height;
    public final double[] array;

    public void serializeChunk(DataOutputStream pngStream, byte[] tag, byte[] data, int length) throws IOException {
        pngStream.writeInt(length);
        pngStream.write(tag, 0, tag.length);
        pngStream.write(data, 0, length);

        CRC32 crc32 = new CRC32();
        crc32.update(tag);
        crc32.update(data, 0, length);
        long crcExpected = crc32.getValue();
        crcExpected &= 0xffffffff;
        
        pngStream.writeInt((int)crcExpected);
    }

    public byte[] serialize(int bitdepth) throws IOException {
        ByteArrayOutputStream pngBytes = new ByteArrayOutputStream();
        DataOutputStream pngStream = new DataOutputStream(pngBytes);

        int scanlineLength;
        if (bitdepth == 8) {
            scanlineLength = width + 1;
        }
        else if (bitdepth == 16) {
            scanlineLength = 2 * width + 1;
        }
        else {
            throw new IOException("bitdepth can only be 8 or 16");
        }

        pngStream.write(PNG_HEADER, 0, PNG_HEADER.length);

        ByteArrayOutputStream IHDRbytes = new ByteArrayOutputStream();
        DataOutputStream IHDRstream = new DataOutputStream(IHDRbytes);
        IHDRstream.writeInt(width);
        IHDRstream.writeInt(height);
        IHDRstream.writeByte(bitdepth);
        IHDRstream.writeByte(0);
        IHDRstream.writeByte(0);
        IHDRstream.writeByte(0);
        IHDRstream.writeByte(0);
        serializeChunk(pngStream, IHDR, IHDRbytes.toByteArray(), 13);

        ByteArrayOutputStream IDATbytes = new ByteArrayOutputStream();
        DataOutputStream IDATstream = new DataOutputStream(IDATbytes);

        for (int row = 0;  row < height;  row++) {
            IDATstream.writeByte(0);

            for (int col = 0;  col < width;  col++) {
                int value = (int)array[row*width + col];
                if (bitdepth == 8) {
                    if (value < 0) {
                        IDATstream.writeByte(0);
                    }
                    else if (value > 0xff) {
                        IDATstream.writeByte(0xff);
                    }
                    else if (value >= 0x80) {
                        IDATstream.writeByte(value - 0x100);
                    }
                    else {
                        IDATstream.writeByte(value);
                    }
                }
                else {
                    if (value < 0) {
                        IDATstream.writeShort(0);
                    }
                    else if (value > 0xffff) {
                        IDATstream.writeShort(0xffff);
                    }
                    else if (value >= 0x8000) {
                        IDATstream.writeShort(value - 0x10000);
                    }
                    else {
                        IDATstream.writeShort(value);
                    }
                }
            }
        }

        byte[] data = IDATbytes.toByteArray();
        Deflater compressor = new Deflater();
        compressor.setInput(data);
        compressor.finish();

        byte[] compressed = new byte[2 * data.length];
        int length = compressor.deflate(compressed);
        serializeChunk(pngStream, IDAT, compressed, length);

        serializeChunk(pngStream, IEND, new byte[0], 0);

        return pngBytes.toByteArray();
    }

    protected byte[] deserializeChunk(DataInputStream pngStream, byte[] assertTag) throws IOException {
        int length = pngStream.readInt();

        byte[] tag = new byte[4];
        pngStream.read(tag, 0, 4);
        if (!Arrays.equals(tag, assertTag)) {
            throw new IOException("unexpected tag in PNG");
        }

        byte[] data = new byte[length];
        pngStream.read(data, 0, length);
        
        CRC32 crc32 = new CRC32();
        crc32.update(tag);
        crc32.update(data);
        long crcExpected = crc32.getValue();
        crcExpected &= 0xffffffff;
        
        int crcObservedSigned = pngStream.readInt();
        long crcObserved = crcObservedSigned >= 0 ? crcObservedSigned : 0x100000000L + crcObservedSigned;

        if (crcExpected != crcObserved) {
            throw new IOException("cyclic redundancy check does not match");
        }

        return data;
    }

    public ImageBandArray(byte[] pngBytes) throws IOException {
        DataInputStream pngStream = new DataInputStream(new ByteArrayInputStream(pngBytes));

        byte[] header = new byte[8];
        pngStream.read(header, 0, 8);

        if (!Arrays.equals(header, PNG_HEADER)) {
            throw new IOException("not a valid PNG file (is it base64 encoded?)");
        }

        byte[] IHDRdata = deserializeChunk(pngStream, IHDR);
        DataInputStream IHDRstream = new DataInputStream(new ByteArrayInputStream(IHDRdata));
        width = IHDRstream.readInt();
        height = IHDRstream.readInt();
        byte bitdepth = IHDRstream.readByte();
        byte colorType = IHDRstream.readByte();
        byte compressionMethod = IHDRstream.readByte();
        byte filterMethod = IHDRstream.readByte();
        byte interlaceMethod = IHDRstream.readByte();

        if (width <= 0  ||  height <= 0) {
            throw new IOException("non-positive width or height");
        }
        if (bitdepth != 8  &&  bitdepth != 16) {
            throw new IOException("only bit depths of 8 or 16 are supported");
        }
        if (colorType != 0) {
            throw new IOException("only greyscale PNG is supported");
        }
        if (compressionMethod != 0  ||  filterMethod != 0  || interlaceMethod != 0) {
            throw new IOException("non-zlib compression, filtering, and interlacing are all not supported");
        }

        int scanlineLength;
        if (bitdepth == 8) {
            scanlineLength = width + 1;
        }
        else {
            scanlineLength = (2 * width) + 1;
        }
        int numBytes = scanlineLength * height;

        byte[] IDATdata = deserializeChunk(pngStream, IDAT);
        Inflater decompressor = new Inflater();
        decompressor.setInput(IDATdata, 0, IDATdata.length);
        byte[] scanlineBytes = new byte[numBytes];
        try {
            decompressor.inflate(scanlineBytes);
        }
        catch (DataFormatException err) {
            throw new IOException(err);
        }
        decompressor.end();
        DataInputStream scanlineStream = new DataInputStream(new ByteArrayInputStream(scanlineBytes));

        array = new double[width * height];
        for (int row = 0;  row < height;  row++) {
            byte filter = scanlineStream.readByte();

            if (filter != (byte)0) {
                throw new IOException("scanline filter is not supported");
            }
            
            for (int col = 0;  col < width;  col++) {
                int value;
                if (bitdepth == 8) {
                    value = scanlineStream.readUnsignedByte();
                }
                else {
                    value = scanlineStream.readUnsignedShort();
                }

                array[row*width + col] = value;
            }
        }

        deserializeChunk(pngStream, IEND);
    }

    public ImageBandArray(int w, int h, double[] inputArray) {
        width = w;
        height = h;
        array = new double[width * height];
        for (int i = 0;  i < w*h;  i++) {
            array[i] = inputArray[i];
        }
    }

    public ImageBandArray(int w, int h, int[] inputArray) {
        width = w;
        height = h;
        array = new double[width * height];
        for (int i = 0;  i < w*h;  i++) {
            array[i] = inputArray[i];
        }
    }

    public ImageBandArray add(ImageBandArray that, double thisscale, double thatscale) {
        double[] data = new double[width * height];
        for (int i = 0;  i < width * height;  i += 1)
            data[i] = (thisscale * array[i]) + (thatscale * that.array[i]);
        return new ImageBandArray(width, height, data);
    }

    public ImageBandArray mul(double scale) {
        double[] data = new double[width * height];
        for (int i = 0;  i < width * height;  i += 1)
            data[i] = scale * array[i];
        return new ImageBandArray(width, height, data);
    }

    public ImageBandArray div(ImageBandArray that) {
        double[] data = new double[width * height];
        for (int i = 0;  i < width * height;  i += 1)
            if (that.array[i] > 0.0)
                data[i] = array[i] / that.array[i];
            else
                data[i] = 0.0;
        return new ImageBandArray(width, height, data);
    }

    public void iadd(ImageBandArray that, double scale) {
        for (int i = 0;  i < width * height;  i += 1)
            array[i] += that.array[i] * scale;
    }

    public void imul(double factor) {
        for (int i = 0;  i < width * height;  i += 1)
            array[i] *= factor;
    }

    public double[] maskedBand(ImageBandArray mask, double threshold) {
        int maskedPixels = 0;
        for (int i = 0;  i < mask.width * mask.height;  i += 1)
            if (mask.array[i] > threshold)
                maskedPixels += 1;
        
        double[] out = new double[maskedPixels];
        int j = 0;
        for (int i = 0;  i < mask.width * mask.height;  i += 1) {
            if (mask.array[i] > threshold) {
                out[j] = array[i];
                j += 1;
            }
        }

        return out;
    }

    static public ImageBandArray unmaskBand(double[] masked, ImageBandArray mask, double threshold) {
        double[] full = new double[mask.width * mask.height];
        int j = 0;
        for (int i = 0;  i < mask.width * mask.height;  i += 1) {
            if (mask.array[i] > threshold) {
                full[i] = masked[j];
                j += 1;
            }
            else
                full[i] = 0.0;
        }

        return new ImageBandArray(mask.width, mask.height, full);
    }

    static public class Corners {
        public int leftX = -1;
        public int leftY = -1;
        public int topX = -1;
        public int topY = -1;
        public int rightX = -1;
        public int rightY = -1;
        public int bottomX = -1;
        public int bottomY = -1;

        public String toString() {
            return String.format("[[%d, %d], [%d, %d], [%d, %d], [%d, %d]]", leftX, leftY, topX, topY, rightX, rightY, bottomX, bottomY);
        }
    }

    public Corners findCorners(double threshold) {
        Corners result = new Corners();

        for (int col = 0;  col < width;  col++) {
            double numer = 0.0;
            double denom = 0.0;
            for (int row = 0;  row < height;  row++) {
                double value = array[row*width + col];
                if (value > threshold) {
                    numer += row;
                    denom += 1.0;
                }
            }
            if (denom > 0.0) {
                result.leftX = col;
                result.leftY = (int)Math.round(numer/denom);
                break;
            }
        }

        for (int row = 0;  row < height;  row++) {
            double numer = 0.0;
            double denom = 0.0;
            for (int col = 0;  col < width;  col++) {
                double value = array[row*width + col];
                if (value > threshold) {
                    numer += col;
                    denom += 1.0;
                }
            }
            if (denom > 0.0) {
                result.topX = (int)Math.round(numer/denom);
                result.topY = row;
                break;
            }
        }

        for (int col = width - 1;  col >= 0;  col--) {
            double numer = 0.0;
            double denom = 0.0;
            for (int row = 0;  row < height;  row++) {
                double value = array[row*width + col];
                if (value > threshold) {
                    numer += row;
                    denom += 1.0;
                }
            }
            if (denom > 0.0) {
                result.rightX = col;
                result.rightY = (int)Math.round(numer/denom);
                break;
            }
        }

        for (int row = height - 1;  row >= 0;  row--) {
            double numer = 0.0;
            double denom = 0.0;
            for (int col = 0;  col < width;  col++) {
                double value = array[row*width + col];
                if (value > threshold) {
                    numer += col;
                    denom += 1.0;
                }
            }
            if (denom > 0.0) {
                result.bottomX = (int)Math.round(numer/denom);
                result.bottomY = row;
                break;
            }
        }

        return result;
    }

    public double averageRadiance() {
        return averageRadiance(this, 0.0);
    }

    public double averageRadiance(ImageBandArray mask, double maskThreshold) {
        if (mask.width != this.width  ||  mask.height != this.height)
            throw new IllegalArgumentException(String.format("width and height of the mask (%d %d) does not fit this image (%d %d)", mask.width, mask.height, this.width, this.height));

        double numer = 0.0;
        double denom = 0.0;
        for (int i = 0;  i < array.length;  i++) {
            if (mask.array[i] > maskThreshold) {
                numer += this.array[i];
                denom += 1.0;
            }
        }

        if (denom == 0.0)
            return 0.0;
        else
            return numer/denom;
    }
}
