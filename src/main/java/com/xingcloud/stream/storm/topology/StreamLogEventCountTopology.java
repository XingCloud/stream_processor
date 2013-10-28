package com.xingcloud.stream.storm.topology;

import static com.xingcloud.stream.StreamProcessorUtils.toBoltId;
import static com.xingcloud.stream.StreamProcessorUtils.toSpoutId;
import static com.xingcloud.stream.StreamProcessorUtils.toTopologyId;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.xingcloud.stream.storm.bolt.EventCountHistoryBolt;
import com.xingcloud.stream.storm.bolt.StreamLogDispatchBolt;
import com.xingcloud.stream.storm.spout.StreamLogReadSpout;
import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 13-10-22 Time: 上午10:57 Package: com.xingcloud.storm.topology
 */
public class StreamLogEventCountTopology extends BaseTopology {
  private static final Logger LOGGER = Logger.getLogger(StreamLogEventCountTopology.class);

  private static boolean debug = true;

  public static void main(String[] args) throws InterruptedException {
    String topoKeyword = "event_count";
    String spoutName = "stream_log_tail_receiver";
    String dispatchBolt = "LogDispatcher";
    String historyCounterBoltName = "history_counter";
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(toSpoutId(topoKeyword, spoutName), new StreamLogReadSpout(), 2);
    builder.setBolt(toBoltId(topoKeyword, dispatchBolt), new StreamLogDispatchBolt())
           .shuffleGrouping(toSpoutId(topoKeyword, spoutName));
    builder.setBolt(toBoltId(topoKeyword, historyCounterBoltName), new EventCountHistoryBolt())
           .fieldsGrouping(toBoltId(topoKeyword, dispatchBolt), "count_history", new Fields("count_history"));

    Config conf = new Config();
    conf.put("debug", false);
    StormTopology topology = builder.createTopology();
    LOGGER.info("[TOPOLOGY] - Topoloty(" + topoKeyword + ") created.");
    if (debug) {
      runTopologyLocal(toTopologyId(topoKeyword), conf, topology, 0);
    } else {

    }
  }

}
