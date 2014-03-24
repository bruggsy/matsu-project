package com.opendatagroup.hyperspectral.newimage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonNode;

// import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloException;
// import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloSecurityException;
// import com.opendatagroup.hyperspectral.mockaccumulo.Connector;
// import com.opendatagroup.hyperspectral.mockaccumulo.Constants;
// import com.opendatagroup.hyperspectral.mockaccumulo.Instance;
// import com.opendatagroup.hyperspectral.mockaccumulo.Key;
// import com.opendatagroup.hyperspectral.mockaccumulo.Range;
// import com.opendatagroup.hyperspectral.mockaccumulo.Scanner;
// import com.opendatagroup.hyperspectral.mockaccumulo.TableExistsException;
// import com.opendatagroup.hyperspectral.mockaccumulo.TableNotFoundException;
// import com.opendatagroup.hyperspectral.mockaccumulo.Value;
// import com.opendatagroup.hyperspectral.mockaccumulo.ZooKeeperInstance;

import org.apache.hadoop.io.Text;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;

public class NewImageScan {
    Logger logger = Logger.getLogger(NewImageScan.class);

    Instance zooKeeperInstance = null;
    Connector connector = null;

    Map<String, Status> fileList = new HashMap<String, Status>();

    public static class Status {
        public int conversionAttempts = 0;
        public boolean successfullyConverted = false;
        public String hdfsFileName = "";

        public String toString() {
            return String.format("{\"conversionAttempts\": %d, \"successfullyConverted\": %s, \"hdfsFileName\": \"%s\"}", conversionAttempts, (successfullyConverted ? "true" : "false"), hdfsFileName);
        }
    }

    public void openConnector(String dbName, String zooKeeperList, String userName, String password) {
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
    
    public void refreshList(String rootDir, Pattern pattern, String tableName, String localRoot, String stream, SpoutOutputCollector collector, int maximumNumberOfAttempts) {
        Scanner scanner = null;
        try {
            if (!connector.tableOperations().exists(tableName))
                connector.tableOperations().create(tableName);
            scanner = connector.createScanner(tableName, Constants.NO_AUTHS);
        }
        catch (AccumuloException exception) {
            throw new RuntimeException("AccumuloException: " + exception.toString());
        }
        catch (AccumuloSecurityException exception) {
            throw new RuntimeException("AccumuloSecurityException: " + exception.toString());
        }
        catch (TableExistsException exception) {
            throw new RuntimeException("TableExistsException: " + exception.toString());
        }
        catch (TableNotFoundException exception) {
            throw new RuntimeException("TableNotFoundException: " + exception.toString());
        }

        scanner.setRange(new Range());
        
        ObjectMapper objectMapper = new ObjectMapper();

        fileList = new HashMap<String, Status>();
        for (Map.Entry<Key, Value> entry : scanner) {
            String key = entry.getKey().getRow().toString();
            String value = new String(entry.getValue().get());

            JsonNode rootNode = null;
            try {
                rootNode = objectMapper.readTree(value);
            }
            catch (IOException exception) {
                throw new RuntimeException("IOException: " + exception.toString());
            }

            Status status = new Status();
            status.conversionAttempts = rootNode.path("conversionAttempts").getIntValue();
            status.successfullyConverted = rootNode.path("successfullyConverted").getBooleanValue();
            status.hdfsFileName = rootNode.path("hdfsFileName").getTextValue();
            fileList.put(key, status);

            logger.info("from db: " + key + " " + status.toString());

            if (status.conversionAttempts < maximumNumberOfAttempts  &&  !status.successfullyConverted) {
                logger.info("    retrying");
                collector.emit(stream, new Values(key, key.replace(localRoot, "") + ".seqpng"));
            }
            else
                logger.info("    skipping");
        }

        recursiveSearch(new File(rootDir), pattern, localRoot, stream, collector, maximumNumberOfAttempts);
    }

    void recursiveSearch(File dir, Pattern pattern, String localRoot, String stream, SpoutOutputCollector collector, int maximumNumberOfAttempts) {
        File[] dirFiles = dir.listFiles();
        if (dirFiles != null) {
            for (File file : dirFiles) {
                Matcher matcher = pattern.matcher(file.getName());
                if (matcher.find()) {
                    if (!fileList.containsKey(file.getAbsolutePath())) {
                        Status status = new Status();
                        fileList.put(file.getAbsolutePath(), status);
                        if (status.conversionAttempts < maximumNumberOfAttempts  &&  !status.successfullyConverted) {
                            collector.emit(stream, new Values(file.getAbsolutePath(), file.getAbsolutePath().replace(localRoot, "") + ".seqpng"));
                        }
                    }
                }
                else if (file.isDirectory()) {
                    recursiveSearch(file, pattern, localRoot, stream, collector, maximumNumberOfAttempts);
                }
            }
        }
    }

    public List<String> imagesToConvert(int maximumNumberOfAttempts) {
        List<String> out = new ArrayList<String>();
        for (Map.Entry<String, Status> entry : fileList.entrySet()) {
            if (entry.getValue().conversionAttempts < maximumNumberOfAttempts  &&  !entry.getValue().successfullyConverted) {
                out.add(entry.getKey());
            }
        }
        return out;
    }
}
