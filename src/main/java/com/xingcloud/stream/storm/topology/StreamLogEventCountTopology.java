package com.xingcloud.stream.storm.topology;

import static com.xingcloud.stream.storm.StreamProcessorUtils.toBoltId;
import static com.xingcloud.stream.storm.StreamProcessorUtils.toSpoutId;
import static com.xingcloud.stream.storm.StreamProcessorUtils.toTopologyId;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.xingcloud.stream.storm.StreamProcessorConstants;
import com.xingcloud.stream.storm.bolt.EventCountHistoryBolt;
import com.xingcloud.stream.storm.bolt.ShuffleBolt;
import com.xingcloud.stream.storm.spout.StreamLogReadSpout;
import com.xingcloud.stream.tailer.StreamLogTailer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * User: Z J Wu Date: 13-10-22 Time: 上午10:57 Package: com.xingcloud.storm.topology
 */
public class StreamLogEventCountTopology extends BaseTopology {
  private static final Log LOG = LogFactory.getLog(StreamLogEventCountTopology.class);

  private static boolean debug = true;

  public static void main(String[] args) throws InterruptedException {

    TopologyBuilder builder = new TopologyBuilder();

    Thread tailerThread = new Thread() {
      @Override
      public synchronized void run() {
        StreamLogTailer streamLogTailer = new StreamLogTailer(StreamLogTailer.configPath);
        try {
          streamLogTailer.start();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    tailerThread.start();
    LOG.info("Init stream log tailer thread finish.");

    builder.setSpout(toSpoutId(StreamProcessorConstants.topoKeyword, StreamProcessorConstants.spoutName), new StreamLogReadSpout(), 4);
    builder.setBolt(toBoltId(StreamProcessorConstants.topoKeyword, StreamProcessorConstants.shuffleBoltName), new ShuffleBolt(), 4)
            .shuffleGrouping(toSpoutId(StreamProcessorConstants.topoKeyword, StreamProcessorConstants.spoutName));
    builder.setBolt(toBoltId(StreamProcessorConstants.topoKeyword, StreamProcessorConstants.historyCounterBoltName),
            new EventCountHistoryBolt(), 4)
            .fieldsGrouping(toBoltId(StreamProcessorConstants.topoKeyword, StreamProcessorConstants.shuffleBoltName),
                    new Fields(StreamProcessorConstants.PID, StreamProcessorConstants.EVENT_NAME));


    StormTopology topology = builder.createTopology();
    LOG.info("[TOPOLOGY] - Topoloty(" + StreamProcessorConstants.topoKeyword + ") created.");
    if (debug) {
      Config conf = new Config();
      conf.setDebug(true);
      runTopologyLocal(toTopologyId(StreamProcessorConstants.topoKeyword), conf, topology);
    } else {

    }
  }

}
