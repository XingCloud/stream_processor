package com.xingcloud.stream.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.xingcloud.stream.model.StreamLogContent;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * User: Z J Wu Date: 13-10-22 Time: 上午9:53 Package: com.xingcloud.storm.bolt
 */
public class StreamLogDispatchBolt extends BaseRichBolt {
  private static final Logger LOGGER = Logger.getLogger(StreamLogDispatchBolt.class);

  private OutputCollector collector;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
    LOGGER.info("[BOLT] - Bolt inited.");
  }

  @Override
  public void execute(Tuple param) {
    Object obj = param.getValue(0);
    if (!StreamLogContent.isStremLogContent(obj)) {
      return;
    }
    StreamLogContent slc = (StreamLogContent) obj;
    // param is also an anchor.
    collector.emit("count_history", param, new Values(slc));
    collector.ack(param);
//    LOGGER.info("[BOLT] - Dispatcher received SLC and emitted it again.");
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream("count_history", new Fields("count_history"));
  }

}
