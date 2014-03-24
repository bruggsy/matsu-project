package com.opendatagroup.hyperspectral.newimage;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.typedbytes.TypedBytesInput;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

import com.opendatagroup.hyperspectral.seqpng.BinaryHadoop;

public class TypedBytesToSequenceFile {
    SequenceFile.Writer writer = null;

    public TypedBytesToSequenceFile(String uri) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Path path = new Path(uri);
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path))
            fs.delete(path, false);
        writer = SequenceFile.createWriter(fs, conf, path, TypedBytesWritable.class, TypedBytesWritable.class);
    }

    public void write(byte[] key, byte[] value, byte valueType) throws IOException {
        ByteArrayOutputStream keyHeaderBAOS = new ByteArrayOutputStream();
        DataOutputStream keyHeaderDOS = new DataOutputStream(keyHeaderBAOS);
        keyHeaderDOS.writeByte(0);
        keyHeaderDOS.writeInt(key.length + 5);
        keyHeaderDOS.writeByte(0);
        keyHeaderDOS.writeInt(key.length);
        keyHeaderDOS.close();
        byte[] keyHeader = keyHeaderBAOS.toByteArray();

        ByteArrayOutputStream valueHeaderBAOS = new ByteArrayOutputStream();
        DataOutputStream valueHeaderDOS = new DataOutputStream(valueHeaderBAOS);
        valueHeaderDOS.writeByte(valueType);
        valueHeaderDOS.writeInt(value.length + 5);
        valueHeaderDOS.writeByte(valueType);
        valueHeaderDOS.writeInt(value.length);
        valueHeaderDOS.close();
        byte[] valueHeader = valueHeaderBAOS.toByteArray();

        writeRaw(keyHeader, key, valueHeader, value);
    }

    private void writeRaw(byte[] keyHeader, byte[] rawKey, byte[] valueHeader, byte[] rawValue) throws IOException {
        TypedBytesWritable key = new TypedBytesWritable();
        TypedBytesWritable value = new TypedBytesWritable();

        byte[] wholeKey = new byte[keyHeader.length + rawKey.length];
        System.arraycopy(keyHeader, 0, wholeKey, 0, keyHeader.length);
        System.arraycopy(rawKey, 0, wholeKey, keyHeader.length, rawKey.length);

        byte[] wholeValue = new byte[valueHeader.length + rawValue.length];
        System.arraycopy(valueHeader, 0, wholeValue, 0, valueHeader.length);
        System.arraycopy(rawValue, 0, wholeValue, valueHeader.length, rawValue.length);

        key.set(wholeKey, 0, wholeKey.length);
        value.set(wholeValue, 0, wholeValue.length);
        writer.append(key, value);
    }

    public void close() throws IOException {
        writer.close();
    }
}
