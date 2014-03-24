package com.opendatagroup.hyperspectral.newimage;

import java.util.Map;

import org.apache.log4j.Logger;

import org.codehaus.jackson.map.ObjectMapper;

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

public class SummaryStatistics {
    public static class TestSpout extends BaseRichSpout {
        Logger logger = null;
        SpoutOutputCollector spoutOutputCollector = null;

        String[] glusterList = new String[]{"/glusterfs/osdc_public_data/eo1/hyperion_l1g/2013/300/EO1H1210442013300110T2_HYP_L1G"};
        String[] hdfsList = new String[]{"ConvertedImages-hyperion/2013/300/EO1H1210442013300110T2_HYP_L1G.seqpng"};
        int index = 0;

        @Override public void open(Map stormConf, TopologyContext topologyContext, SpoutOutputCollector _spoutOutputCollector) {
            logger = Logger.getLogger(TestSpout.class);
            spoutOutputCollector = _spoutOutputCollector;
        }

        @Override public void nextTuple() {
            if (index < glusterList.length) {
                logger.info("starting fileName " + glusterList[index] + " " + hdfsList[index]);
                spoutOutputCollector.emit("hdfsFileNames", new Values(glusterList[index], hdfsList[index]));
                index += 1;
            }
        }

        @Override public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declareStream("fileNames", new Fields("localName", "hdfsName"));
        }
    }

    public static class SummaryStatisticsFromPython extends ShellBolt implements IRichBolt {
        public SummaryStatisticsFromPython() {
            super("python", "python/SummaryStatisticsFromPython.py");
        }

        @Override public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declareStream("summaryStatistics", new Fields("localName", "hdfsName", "result"));
        }

        @Override public Map<String, Object> getComponentConfiguration() { return null; }
    }

    public static class SummaryStatisticsToAccumulo extends BaseRichBolt {
        String dbName = null;
        String zooKeeperList = null;
        String accumuloUser = null;
        String accumuloPassword = null;
        String tableName = null;

        OutputCollector outputCollector = null;
        Logger logger = null;
        ObjectMapper objectMapper = null;

        BatchWriter batchWriter = null;

        public SummaryStatisticsToAccumulo(String _dbName, String _zooKeeperList, String _accumuloUser, String _accumuloPassword, String _tableName) {
            dbName = _dbName;
            zooKeeperList = _zooKeeperList;
            accumuloUser = _accumuloUser;
            accumuloPassword = _accumuloPassword;
            tableName = _tableName;
        }

        @Override public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector _outputCollector) {
            outputCollector = _outputCollector;
            logger = Logger.getLogger(SummaryStatisticsToAccumulo.class);
            objectMapper = new ObjectMapper();

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
            catch (TableNotFoundException exception) {
                throw new RuntimeException("TableNotFoundException: " + exception.toString());
            }
            catch (TableExistsException exception) {
                throw new RuntimeException("TableExistsException: " + exception.toString());
            }
        }

        @Override public void execute(Tuple tuple) {
            String localName = tuple.getStringByField("localName");
            String hdfsName = tuple.getStringByField("hdfsName");
            String result = tuple.getStringByField("result");

            logger.info("uploading data for " + hdfsName + " to Accumulo: " + result);

            try {
                objectMapper.readTree(result);
            }
            catch (Exception exception) {
                throw new RuntimeException("bad JSON: " + result);
            }

            Mutation mutation = new Mutation(new Text(localName));
            mutation.put(new Text(""), new Text(""), new Value(result.getBytes()));
            try {
                batchWriter.addMutation(mutation);
            }
            catch (MutationsRejectedException exception) {
                throw new RuntimeException("MutationsRejectedException: " + exception.toString());
            }
        }

        @Override public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
    }

    public static void main(String[] argv) throws Exception {
        String dbName = "accumulo";
        String zooKeeperList = "1.2.3.4:2181";
        String accumuloUser = "root";
        String accumuloPassword = "YouKnowThisIsAFakePasswordRight?";
        String tableName = "SummaryStatistics-hyperion";

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("TestSpout", new TestSpout(), 1);
        topologyBuilder.setBolt("SummaryStatisticsFromPython", new SummaryStatisticsFromPython(), 1).shuffleGrouping("TestSpout", "fileNames");
        topologyBuilder.setBolt("SummaryStatisticsToAccumulo", new SummaryStatisticsToAccumulo(dbName, zooKeeperList, accumuloUser, accumuloPassword, tableName), 1).shuffleGrouping("SummaryStatisticsFromPython", "summaryStatistics");

        Config config = new Config();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("TestSummaryStatistics", config, topologyBuilder.createTopology());

        while (true)
            Thread.sleep(1000);
    }
}
