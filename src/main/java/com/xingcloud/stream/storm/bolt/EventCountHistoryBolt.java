package com.xingcloud.stream.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.xingcloud.stream.model.EventCounterUpdater;
import com.xingcloud.stream.model.StreamLogContent;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * User: Z J Wu Date: 13-10-22 Time: 上午9:55 Package: com.xingcloud.storm.bolt
 */
public class EventCountHistoryBolt extends BaseEventCountBolt {
  private static final Logger LOGGER = Logger.getLogger(EventCountHistoryBolt.class);

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    StreamLogContent slc = (StreamLogContent) tuple.getValue(0);
    String k = slc.toKey();
    EventCounterUpdater ec = counter.get(k);
    if (ec == null) {
      counter.put(k, new EventCounterUpdater(slc));
    } else {
      ec.update(slc.getEventValue(), slc.getTimestamp());
    }
    for (Map.Entry<String, EventCounterUpdater> entry : counter.entrySet()) {
      LOGGER.info("[BOLT]: " + entry.getValue());
    }
    collector.ack(tuple);
//    LOGGER.info("[BOLT] - Historical counter received SLC - " + slc);
  }
}
