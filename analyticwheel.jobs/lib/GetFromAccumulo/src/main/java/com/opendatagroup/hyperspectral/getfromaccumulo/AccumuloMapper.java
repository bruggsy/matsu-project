package com.opendatagroup.hyperspectral.getfromaccumulo;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.HashMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
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

public class AccumuloMapper implements Mapper<LongWritable, Text, Text, Text> {

    Instance zooKeeperInstance = null;
    Connector connector = null;
    String tableName;

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

    public void map(LongWritable key, Text fileName, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        Map<String, String> map2 = readfile("config");
        String dbName = map2.get("accumulo.dbName");
        String zooKeeperList = map2.get("accumulo.zooKeeperList");
        String userName = map2.get("accumulo.userName");
        String password = map2.get("accumulo.password");

        if (connector == null) {
            try {
                zooKeeperInstance = new ZooKeeperInstance(dbName, zooKeeperList);
                if (zooKeeperInstance == null) {
                    throw new RuntimeException("Could not connect to ZooKeeper " + dbName + " " + zooKeeperList);
                }
                connector = zooKeeperInstance.getConnector(userName, password.getBytes());
            }
            catch (AccumuloException exception) {
                throw new RuntimeException("AccumuloException: " + exception.toString());
            }
            catch (AccumuloSecurityException exception) {
                throw new RuntimeException("AccumuloSecurityException: " + exception.toString());
            }
        }

        Scanner scanner = null;
        try {
            scanner = connector.createScanner(tableName, Constants.NO_AUTHS);
        }
        // catch (AccumuloException exception) {
        //     throw new RuntimeException("AccumuloException: " + exception.toString());
        // }
        // catch (AccumuloSecurityException exception) {
        //     throw new RuntimeException("AccumuloSecurityException: " + exception.toString());
        // }
        catch (TableNotFoundException exception) {
            throw new RuntimeException("TableNotFoundException: " + exception.toString());
        }

        scanner.setRange(new Range(fileName.toString(), fileName.toString()));
        for (Map.Entry<Key, Value> entry : scanner) {
            outputCollector.collect(new Text("ONLYKEY"), new Text(entry.getValue().get()));
        }
    }
}

