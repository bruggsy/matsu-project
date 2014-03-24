package com.opendatagroup.hyperspectral.seqpng;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;

import com.opendatagroup.hyperspectral.seqpng.ImageBandArray;

public class BinaryHadoop {
    public static final int SORTER_LENGTH = 0;
    public static final byte TYPEDBYTES_RAW = 0;
    public static final byte TYPEDBYTES_PNG = 50;
    public static final byte TYPEDBYTES_JSON = 51;
    public static final byte TYPEDBYTES_PICKLE = 52;
    public static final byte TYPEDBYTES_ARRAY = 53;

    public static void emit(String key, String sorter, byte valueTypeCode, byte[] value, OutputCollector<BytesWritable, BytesWritable> output) throws IOException {
        if (SORTER_LENGTH > 0) {
            if (sorter.length() > SORTER_LENGTH) {
                sorter = sorter.substring(0, SORTER_LENGTH);
            }
            else if (sorter.length() < SORTER_LENGTH) {
                int padSpaces = SORTER_LENGTH - sorter.length();
                sorter = String.format("%1$-" + padSpaces + "s", sorter);
            }

            ByteArrayOutputStream outputValueBytes = new ByteArrayOutputStream();
            DataOutputStream outputValueStream = new DataOutputStream(outputValueBytes);
            outputValueStream.writeBytes(sorter);
            outputValueStream.write(value, 0, value.length);
            value = outputValueBytes.toByteArray();
        }

        ByteArrayOutputStream outputKeyBytes = new ByteArrayOutputStream();
        DataOutputStream outputKeyStream = new DataOutputStream(outputKeyBytes);
        outputKeyStream.writeByte(0);
        outputKeyStream.writeInt(key.length());
        outputKeyStream.writeBytes(key);

        ByteArrayOutputStream outputValueBytes = new ByteArrayOutputStream();
        DataOutputStream outputValueStream = new DataOutputStream(outputValueBytes);
        outputValueStream.writeByte(valueTypeCode);
        outputValueStream.writeInt(value.length);
        outputValueStream.write(value, 0, value.length);

        output.collect(new BytesWritable(outputKeyBytes.toByteArray()), new BytesWritable(outputValueBytes.toByteArray()));
    }

    public static String mapperKey(BytesWritable key) throws IOException {
        byte[] keyBytes = key.getBytes();
        DataInputStream keyStream = new DataInputStream(new ByteArrayInputStream(keyBytes));
        keyStream.skipBytes(9);

        byte typeCode = keyStream.readByte();
        if (typeCode != 0) { throw new IOException("mapper key typecode is not 0"); }

        int length = keyStream.readInt();
        if (length > key.getLength()) { throw new IOException("mapper key length exceeds buffer"); }

        return new String(Arrays.copyOfRange(keyBytes, 14, 14 + length));
    }

    public static String mapperSorter(BytesWritable value) throws IOException {
        byte[] valueBytes = value.getBytes();
        return new String(Arrays.copyOfRange(valueBytes, 14, 14 + SORTER_LENGTH));
    }

    public static byte[] mapperValueRaw(BytesWritable value, byte assertType) throws IOException {
        return mapperValueRaw(value, assertType, 0);
    }

    public static byte[] mapperValueRaw(BytesWritable value, byte assertType, int skipMoreBytes) throws IOException {
        byte[] valueBytes = value.getBytes();
        DataInputStream valueStream = new DataInputStream(new ByteArrayInputStream(valueBytes));
        valueStream.skipBytes(9);

        byte typeCode = valueStream.readByte();
        if (typeCode != assertType) {
            throw new IOException(String.format("mapper value typeCode is %d but %d was expected", typeCode, assertType));
        }

        int length = valueStream.readInt();
        if (length > value.getLength()) { throw new IOException("mapper value length exceeds buffer"); }

        return Arrays.copyOfRange(valueBytes, 14 + SORTER_LENGTH + skipMoreBytes, 14 + SORTER_LENGTH + length);
    }

    public static String mapperValueString(BytesWritable value) throws IOException {
        return new String(mapperValueRaw(value, TYPEDBYTES_RAW));
    }

    public static String mapperValueJSON(BytesWritable value) throws IOException {
        return new String(mapperValueRaw(value, TYPEDBYTES_JSON));  // TODO: return JSON data
    }

    public static ImageBandArray mapperValueImage(BytesWritable value) throws IOException {
        return new ImageBandArray(mapperValueRaw(value, TYPEDBYTES_PNG));
    }

    public static String reducerKey(BytesWritable key) throws IOException {
        byte[] keyBytes = key.getBytes();
        DataInputStream keyStream = new DataInputStream(new ByteArrayInputStream(keyBytes));

        byte typeCode = keyStream.readByte();
        if (typeCode != 0) { throw new IOException("reducer key typecode is not 0"); }

        int length = keyStream.readInt();
        if (length > key.getLength()) { throw new IOException("mapper key length exceeds buffer"); }

        return new String(Arrays.copyOfRange(keyBytes, 5, 5 + length));
    }

    public static String reducerSorter(BytesWritable value) throws IOException {
        byte[] valueBytes = value.getBytes();
        return new String(Arrays.copyOfRange(valueBytes, 5, 5 + SORTER_LENGTH));
    }

    public static byte[] reducerValueRaw(BytesWritable value, byte assertType) throws IOException {
        return reducerValueRaw(value, assertType, 0);
    }

    public static byte[] reducerValueRaw(BytesWritable value, byte assertType, int skipMoreBytes) throws IOException {
        byte[] valueBytes = value.getBytes();
        DataInputStream valueStream = new DataInputStream(new ByteArrayInputStream(valueBytes));

        byte typeCode = valueStream.readByte();
        if (typeCode != assertType) {
            throw new IOException(String.format("mapper value typeCode is %d but %d was expected", typeCode, assertType));
        }

        int length = valueStream.readInt();
        if (length > value.getLength()) { throw new IOException("mapper value length exceeds buffer"); }

        return Arrays.copyOfRange(valueBytes, 5 + SORTER_LENGTH + skipMoreBytes, 5 + SORTER_LENGTH + length);
    }

    public static String reducerValueString(BytesWritable value) throws IOException {
        return new String(reducerValueRaw(value, TYPEDBYTES_RAW));
    }

    public static String reducerValueJSON(BytesWritable value) throws IOException {
        return new String(reducerValueRaw(value, TYPEDBYTES_JSON));   // TODO: return JSON data
    }

    public static ImageBandArray reducerValueImage(BytesWritable value) throws IOException {
        return new ImageBandArray(reducerValueRaw(value, TYPEDBYTES_PNG));
    }
}
