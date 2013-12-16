package com.xingcloud.stream.storm.topology;

import static com.xingcloud.stream.storm.StreamProcessorUtils.toBoltId;
import static com.xingcloud.stream.storm.StreamProcessorUtils.toSpoutId;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.xingcloud.stream.storm.StreamProcessorConstants;
import com.xingcloud.stream.storm.bolt.EventCountHistoryBolt;
import com.xingcloud.stream.storm.bolt.ShuffleBolt;
import com.xingcloud.stream.storm.spout.StreamLogReadSpout;
import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 13-10-22 Time: 上午10:57 Package: com.xingcloud.storm.topology
 */
public class StreamLogEventCountTopology {
  private static final Logger logger = Logger.getLogger(StreamLogEventCountTopology.class);

  public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(
      toSpoutId(StreamProcessorConstants.topoKeyword, StreamProcessorConstants.spoutName),
      new StreamLogReadSpout(), 4
    );

    builder.setBolt(
      toBoltId(StreamProcessorConstants.topoKeyword, StreamProcessorConstants.shuffleBoltName),
      new ShuffleBolt(), 4
    ).shuffleGrouping(
      toSpoutId(StreamProcessorConstants.topoKeyword, StreamProcessorConstants.spoutName));

    builder.setBolt(
      toBoltId(StreamProcessorConstants.topoKeyword, StreamProcessorConstants.historyCounterBoltName),
      new EventCountHistoryBolt(), 4
    ).fieldsGrouping(
      toBoltId(StreamProcessorConstants.topoKeyword, StreamProcessorConstants.shuffleBoltName),
      new Fields(StreamProcessorConstants.PID, StreamProcessorConstants.EVENT_NAME)
    );

    StormTopology topology = builder.createTopology();
    logger.info("[TOPOLOGY] - Topoloty(" + StreamProcessorConstants.topoKeyword + ") created.");
    Config conf = new Config();
    conf.setDebug(false);
    if (args != null && args.length > 0 && args[0].equals(StreamProcessorConstants.topoKeyword)) {
      conf.setNumWorkers(4);
      StormSubmitter.submitTopology(args[0], conf, topology);
    } else {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology(StreamProcessorConstants.topoKeyword, conf, topology);
      Utils.sleep(10*1000);
      cluster.shutdown();
      logger.info("CLuster was killed.");
    }

  }

}
