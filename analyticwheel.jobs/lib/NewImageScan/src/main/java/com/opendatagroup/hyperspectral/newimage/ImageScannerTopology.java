package com.opendatagroup.hyperspectral.newimage;

import java.io.File;
import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.Thread;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonNode;

// import com.opendatagroup.hyperspectral.mockaccumulo.Text;
// import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloException;
// import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloSecurityException;
// import com.opendatagroup.hyperspectral.mockaccumulo.BatchWriter;
// import com.opendatagroup.hyperspectral.mockaccumulo.Connector;
// import com.opendatagroup.hyperspectral.mockaccumulo.Constants;
// import com.opendatagroup.hyperspectral.mockaccumulo.Instance;
// import com.opendatagroup.hyperspectral.mockaccumulo.Key;
// import com.opendatagroup.hyperspectral.mockaccumulo.MultiTableBatchWriter;
// import com.opendatagroup.hyperspectral.mockaccumulo.Mutation;
// import com.opendatagroup.hyperspectral.mockaccumulo.MutationsRejectedException;
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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.opendatagroup.hyperspectral.newimage.NewImageScan;
import com.opendatagroup.hyperspectral.newimage.SummaryStatistics;
import com.opendatagroup.hyperspectral.seqpng.MetadataInterface;

public class ImageScannerTopology {
    public static class GetListOfFiles extends BaseRichSpout {
        String stream = null;
        String dbName = null;
        String zooKeeperList = null;
        String accumuloUser = null;
        String accumuloPassword = null;
        String tableName = null;
        String[] localFileRoot = null;
        String dirNamePattern = null;
        int maxAttempts = 0;
        long millisBetweenScans = 0L;

        Logger logger = null;
        SpoutOutputCollector collector = null;

        public GetListOfFiles(String _stream, String _dbName, String _zooKeeperList, String _accumuloUser, String _accumuloPassword, String _tableName, String _localFileRoot, String _dirNamePattern, int _maxAttempts, long _millisBetweenScans) {
            stream = _stream;
            dbName = _dbName;
            zooKeeperList = _zooKeeperList;
            accumuloUser = _accumuloUser;
            accumuloPassword = _accumuloPassword;
            tableName = _tableName;
            localFileRoot = _localFileRoot.split(":");
            dirNamePattern = _dirNamePattern;
            maxAttempts = _maxAttempts;
            millisBetweenScans = _millisBetweenScans;
        }

        @Override public void open(Map stormConf, TopologyContext context, SpoutOutputCollector _collector) {
            logger = Logger.getLogger(GetListOfFiles.class);
            collector = _collector;

            logger.info("Waiting 10 seconds before starting...");
            try {
                Thread.sleep(10 * 1000);
            }
            catch (InterruptedException exception) {
                throw new RuntimeException("InterruptedException: " + exception.toString());
            }
        }

        @Override public void nextTuple() {
            NewImageScan newImageScan = new NewImageScan();
            newImageScan.openConnector(dbName, zooKeeperList, accumuloUser, accumuloPassword);

            for (String localRoot : localFileRoot) {
                if (!localRoot.equals("")) {
                    newImageScan.refreshList(localRoot, Pattern.compile(dirNamePattern), tableName, localRoot, stream, collector, maxAttempts);
                    // List<String> newImages = newImageScan.imagesToConvert(maxAttempts);
                    // Collections.sort(newImages);
                    // Collections.reverse(newImages);

                    // for (String localDirName : newImages) {
                    //     String hdfsFileName = localDirName.replace(localRoot, "") + ".seqpng";
                    //     collector.emit(stream, new Values(localDirName, hdfsFileName));
                    // }
                }
            }

            try {
                Thread.sleep(millisBetweenScans);
            }
            catch (InterruptedException exception) {
                throw new RuntimeException("InterruptedException: " + exception.toString());
            }
        }

        @Override public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream(stream, new Fields("localDirName", "hdfsFileName"));
        }
    }

    public static class AvailableImagesJson {
        String tableName = null;

        Logger logger = null;
        BatchWriter batchWriter = null;

        public AvailableImagesJson(String dbName, String zooKeeperList, String accumuloUser, String accumuloPassword, String _tableName) {
            tableName = _tableName;

            logger = Logger.getLogger(AvailableImagesJson.class);
            try {
                Instance zooKeeperInstance = new ZooKeeperInstance(dbName, zooKeeperList);
                if (zooKeeperInstance == null) {
                    throw new RuntimeException("Could not connect to ZooKeeper " + dbName + " " + zooKeeperList);
                }
                Connector connector = zooKeeperInstance.getConnector(accumuloUser, accumuloPassword.getBytes());
                if (!connector.tableOperations().exists(tableName))
                    connector.tableOperations().create(tableName);

                MultiTableBatchWriter multiTableBatchWriter = connector.createMultiTableBatchWriter(200000L, 300, 4);
                batchWriter = multiTableBatchWriter.getBatchWriter(tableName);
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
        }

        public void put(String imageKey, String imageMetadata) {
            MetadataInterface metadataInterface = new MetadataInterface(imageMetadata);
            try {
                String availableImagesMetadata = String.format("{\"directory\": \"%s\", \"acquisition_start\": %s, \"radiance\": %s, \"corners\": %s}",
                                                               metadataInterface.getOriginalDirName(),
                                                               metadataInterface.getAcquisitionStart(),
                                                               metadataInterface.getMaxAverageRadiance(),
                                                               metadataInterface.getCornersByLngLat());

                Mutation mutation = new Mutation(new Text(imageKey));
                mutation.put(new Text(""), new Text(""), new Value(availableImagesMetadata.getBytes()));
                try {
                    batchWriter.addMutation(mutation);
                }
                catch (MutationsRejectedException exception) {
                    throw new RuntimeException("MutationsRejectedException: " + exception.toString());
                }

                logger.info(String.format("updated %s table: %s -> %s", tableName, imageKey, availableImagesMetadata));
            }
            catch (NullPointerException exception) {
                logger.error(String.format("could not update %s table: %s START_TIME is \"%s\"", tableName, imageKey, metadataInterface.getStartTime()));
            }
        }
    }

    public static class ConvertImage extends BaseRichBolt {
        String stream = null;
        String dbName = null;
        String zooKeeperList = null;
        String accumuloUser = null;
        String accumuloPassword = null;
        String tableName = null;
        String availableImagesTableName = null;
        String hdfsDirUri = null;
        Pattern metadataFilePattern = null;
        String wavelengthFile = null;
        String multiplierFile = null;

        OutputCollector outputCollector = null;
        Logger logger = null;
        ObjectMapper objectMapper = null;

        Scanner scanner = null;
        BatchWriter batchWriter = null;
        AvailableImagesJson availableImagesJson = null;

        public ConvertImage(String _stream, String _dbName, String _zooKeeperList, String _accumuloUser, String _accumuloPassword, String _tableName, String _availableImagesTableName, String _hdfsDirUri, String _metadataFilePattern, String _wavelengthFile, String _multiplierFile) {
            stream = _stream;
            dbName = _dbName;
            zooKeeperList = _zooKeeperList;
            accumuloUser = _accumuloUser;
            accumuloPassword = _accumuloPassword;
            tableName = _tableName;
            availableImagesTableName = _availableImagesTableName;
            hdfsDirUri = _hdfsDirUri;
            metadataFilePattern = Pattern.compile(_metadataFilePattern);
            wavelengthFile = _wavelengthFile;
            multiplierFile = _multiplierFile;
        }

        @Override public void prepare(Map stormConf, TopologyContext context, OutputCollector _outputCollector) {
            outputCollector = _outputCollector;
            logger = Logger.getLogger(ConvertImage.class);
            objectMapper = new ObjectMapper();

            try {
                Instance zooKeeperInstance = new ZooKeeperInstance(dbName, zooKeeperList);
                if (zooKeeperInstance == null) {
                    throw new RuntimeException("Could not connect to ZooKeeper " + dbName + " " + zooKeeperList);
                }
                Connector connector = zooKeeperInstance.getConnector(accumuloUser, accumuloPassword.getBytes());
                if (!connector.tableOperations().exists(tableName))
                    connector.tableOperations().create(tableName);

                scanner = connector.createScanner(tableName, Constants.NO_AUTHS);

                MultiTableBatchWriter multiTableBatchWriter = connector.createMultiTableBatchWriter(200000L, 300, 4);
                batchWriter = multiTableBatchWriter.getBatchWriter(tableName);
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

            availableImagesJson = new AvailableImagesJson(dbName, zooKeeperList, accumuloUser, accumuloPassword, availableImagesTableName);
        }

        NewImageScan.Status getStatus(String imageDirName) {
            String currentStatus = null;
            scanner.setRange(new Range(imageDirName, imageDirName));
            for (Map.Entry<Key, Value> entry : scanner) {
                currentStatus = new String(entry.getValue().get());
            }

            if (currentStatus == null)
                return new NewImageScan.Status();

            JsonNode rootNode = null;
            try {
                rootNode = objectMapper.readTree(currentStatus);
            }
            catch (IOException exception) {
                throw new RuntimeException("IOException: " + exception.toString());
            }

            NewImageScan.Status status = new NewImageScan.Status();            
            status.conversionAttempts = rootNode.path("conversionAttempts").getIntValue();
            status.successfullyConverted = rootNode.path("successfullyConverted").getBooleanValue();
            status.hdfsFileName = rootNode.path("hdfsFileName").getTextValue();

            return status;
        }

        @Override public void execute(Tuple tuple) {
            String localDirName = tuple.getStringByField("localDirName");
            String hdfsFileName = hdfsDirUri + tuple.getStringByField("hdfsFileName");

            NewImageScan.Status status = getStatus(localDirName);

            logger.info("attempting to convert: " + localDirName + " to " + hdfsFileName);
            String result = LocalDirToHdfsFile.doGeoTiff(localDirName, hdfsFileName, metadataFilePattern, wavelengthFile, multiplierFile);

            if (result.startsWith("SUCCESS")) {
                logger.info("successfully converted: " + localDirName + " to " + hdfsFileName);
                status.conversionAttempts += 1;
                status.successfullyConverted = true;
                status.hdfsFileName = hdfsFileName;

                availableImagesJson.put(localDirName, result.substring(8));
                outputCollector.emit("fileNames", new Values(localDirName, hdfsFileName));
            }
            else {
                logger.error("Could not convert \"" + localDirName + "\" because of\n" + result);
                status.conversionAttempts += 1;
                status.successfullyConverted = false;
                status.hdfsFileName = "";
            }

            Mutation mutation = new Mutation(new Text(localDirName));
            mutation.put(new Text(""), new Text(""), new Value(status.toString().getBytes()));
            try {
                batchWriter.addMutation(mutation);
            }
            catch (MutationsRejectedException exception) {
                throw new RuntimeException("MutationsRejectedException: " + exception.toString());
            }

            logger.info(String.format("updated %s table: %s -> %s", tableName, localDirName, status.toString()));
        }

        @Override public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declareStream("fileNames", new Fields("localName", "hdfsName"));
        }
    }

    public static void main(String[] argv) throws Exception {
        if (argv.length != 1)
            throw new IllegalArgumentException("requires one argument (the name of the configuration file)");

        java.util.Scanner configFile = new java.util.Scanner(new File(argv[0])).useDelimiter("\\n");
        Map<String, String> configValues = new java.util.HashMap<String, String>();
        while (configFile.hasNext()) {
            String[] pair = configFile.next().split("\\t");
            if (pair.length == 2)
                configValues.put(pair[0], pair[1]);
        }

        int reloadFrequencyMillis = java.lang.Integer.parseInt(configValues.get("reloadFrequencyMillis"));
        String dbName = configValues.get("dbName");
        String zooKeeperList = configValues.get("zooKeeperList");
        String accumuloUser = configValues.get("accumuloUser");
        String accumuloPassword = configValues.get("accumuloPassword");
        String convertedImageListTableName = configValues.get("convertedImageListTableName");
        String availableImagesTableName = configValues.get("availableImagesTableName");
        String summaryStatisticsTableName = configValues.get("summaryStatisticsTableName");
        String wavelengthFile = configValues.get("wavelengthFile");
        String multiplierFile = configValues.get("multiplierFile");

        String dirNamePattern = configValues.get("dirNamePattern");
        String metadataFilePattern = configValues.get("metadataFilePattern");   // ".*_MTL_L1T\\.TIF" for ALI

        // String localFileRoot = "/glusterfs/osdc_public_data/eo1/hyperion_l1g/2013/300";
        // String hdfsDirUri = "hdfs://127.0.0.1:8020/user/pivarski/ConvertedImages-hyperion/2013/300";
        // String localFileRoot = "/glusterfs/osdc_public_data/eo1/hyperion_l1g/2012";
        // String hdfsDirUri = "hdfs://hu26-h-3:9000/user/hadoop/ConvertedImages-hyperion/2012";
        // String localFileRoot = "/glusterfs/osdc_public_data/eo1/hyperion_l1g/2013";
        // String hdfsDirUri = "hdfs://hu26-h-3:9000/user/hadoop/ConvertedImages-hyperion/2013";
        String localFileRoot = configValues.get("localFileRoot");
        String hdfsDirUri = configValues.get("hdfsDirUri");

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("GetListOfFiles", new GetListOfFiles("hyperion", dbName, zooKeeperList, accumuloUser, accumuloPassword, convertedImageListTableName, localFileRoot, dirNamePattern, 4, reloadFrequencyMillis), 1);
        topologyBuilder.setBolt("ConvertImage", new ConvertImage("hyperion", dbName, zooKeeperList, accumuloUser, accumuloPassword, convertedImageListTableName, availableImagesTableName, hdfsDirUri, metadataFilePattern, wavelengthFile, multiplierFile), 180).shuffleGrouping("GetListOfFiles", "hyperion");
        topologyBuilder.setBolt("SummaryStatisticsFromPython", new SummaryStatistics.SummaryStatisticsFromPython(), 180).shuffleGrouping("ConvertImage", "fileNames");
        topologyBuilder.setBolt("SummaryStatisticsToAccumulo", new SummaryStatistics.SummaryStatisticsToAccumulo(dbName, zooKeeperList, accumuloUser, accumuloPassword, summaryStatisticsTableName), 180).shuffleGrouping("SummaryStatisticsFromPython", "summaryStatistics");

        Config config = new Config();
        config.put(Config.NIMBUS_TASK_LAUNCH_SECS, "3600");
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx4g");   //  -XX:NewRatio=15
        config.setNumWorkers(9);

        // LocalCluster cluster = new LocalCluster();
        // cluster.submitTopology("ImageScannerTopology", config, topologyBuilder.createTopology());
        StormSubmitter.submitTopology("ImageScannerTopology", config, topologyBuilder.createTopology());

        while (true)
            Thread.sleep(1000);
    }
}
