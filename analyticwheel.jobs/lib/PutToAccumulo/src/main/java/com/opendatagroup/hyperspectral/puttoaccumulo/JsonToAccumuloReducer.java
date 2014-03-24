package com.opendatagroup.hyperspectral.puttoaccumulo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.io.FileReader;
import java.io.BufferedReader;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.JobClient;


import com.opendatagroup.hyperspectral.seqpng.BinaryHadoop;

public class JsonToAccumuloReducer extends BinaryHadoop implements Reducer<BytesWritable, BytesWritable, Text, Text> {
    Instance zooKeeperInstance = null;
    Connector connector = null;
    final Text columnFamily = new Text("");
    final Text columnQualifier = new Text("");
    String tableName;
    MultiTableBatchWriter multiTableBatchWriter = null;
    BatchWriter batchWriter = null;

    public void configure(JobConf conf) { 
        tableName = conf.get("accumulotablename");
    }

    public void close() { }


    public Map<String, String> readfile(String file_path) throws IOException {

        FileReader fr = new FileReader(file_path);
        BufferedReader textReader = new BufferedReader(fr);

        Map<String, String> map = new HashMap<String, String>();
        String line = "";
        while ((line = textReader.readLine()) != null) {
            String parts[] = line.split("\t");
            map.put(parts[0], parts[1]);
        }
        textReader.close();
        return map;

    }

    protected void initialize() throws IOException, FileNotFoundException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        Map<String, String> map2 = readfile("config");
        String dbName = map2.get("accumulo.dbName");
        String zooKeeperList = map2.get("accumulo.zooKeeperList");
        String userName = map2.get("accumulo.userName");
        String password = map2.get("accumulo.password");

        if (zooKeeperInstance == null) {
            try {
                System.err.println("Opening zookeeper");
                zooKeeperInstance = new ZooKeeperInstance(dbName, zooKeeperList);
                if (zooKeeperInstance == null) {
	            throw new RuntimeException("Could not connect to ZooKeeper " + dbName + " " + zooKeeperList);
	        }
                connector = zooKeeperInstance.getConnector(userName, password.getBytes());
                System.err.println("Successfully opened");
            }
            catch (AccumuloException exception) {
                throw new RuntimeException("AccumuloException: " + exception.toString());
            }
            catch (AccumuloSecurityException exception) {
                throw new RuntimeException("AccumuloSecurityException: " + exception.toString());
            }
        }

        multiTableBatchWriter = connector.createMultiTableBatchWriter(200000L, 300, 4);
	batchWriter = multiTableBatchWriter.getBatchWriter(tableName);
        System.err.println("Successfully opened batchwriter");
    }

    protected void write(String keyString, byte[] value) throws MutationsRejectedException {
        Mutation mutation = new Mutation(new Text(keyString));
        mutation.put(columnFamily, columnQualifier, new Value(value));
        batchWriter.addMutation(mutation);
    }

    @Override
    public void reduce(BytesWritable key, Iterator<BytesWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        System.err.println("Starting the reduce");
        if (zooKeeperInstance == null  ||  connector == null  ||  multiTableBatchWriter == null  ||  batchWriter == null) {
            try {
                initialize();
            }
            catch (FileNotFoundException e) {
                throw new IOException("FileNotFoundException: " + e.getMessage());
            }
            catch (AccumuloException e) {
                throw new IOException("AccumuloException: " + e.getMessage());
            }
            catch (AccumuloSecurityException e) {
                throw new IOException("AccumuloSecurityException: " + e.getMessage());
            }
            catch (TableExistsException e) {
                throw new IOException("TableExistsException: " + e.getMessage());
            }
            catch (TableNotFoundException e) {
                throw new IOException("TableNotFoundException: " + e.getMessage());
            }
        }

        String keyString = reducerKey(key).substring(5);
        System.err.println("The key is " + keyString);
        byte[] value = null;

        // There ought to be exactly one value.  If there are multiple, take the first; if there are zero, don't write.
        while (values.hasNext()) {
            if (value == null) {
                value = reducerValueRaw(values.next(), BinaryHadoop.TYPEDBYTES_JSON, 5);
                System.err.println("The value is " + new String(value));
            }
        }
        if (value != null) {
            output.collect(new Text(keyString), new Text(value));
            try {
                System.err.println("Before writing");
                write(keyString, value);
                System.err.println("After writing");
            }
            catch (MutationsRejectedException e) {
                throw new IOException("MutationsRejectedException: " + e.getMessage());
            }
        }
        System.err.println("Leaving the reducer");       
    }
}
