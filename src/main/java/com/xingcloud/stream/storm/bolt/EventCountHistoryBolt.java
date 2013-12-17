package com.xingcloud.stream.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.xingcloud.stream.model.EventCounterUpdater;
import com.xingcloud.stream.tailer.TimeUtil;
import org.apache.log4j.Logger;

import java.util.Map;


public class EventCountHistoryBolt extends BaseBasicBolt {
  private static final Logger logger = Logger.getLogger(EventCountHistoryBolt.class);

  private EventCounterUpdater ecu = new EventCounterUpdater();

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    Thread ecuThread = new Thread(ecu);
    ecuThread.start();
    logger.info("EventCountHistoryBolt init bolt finish!");
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String pid = (String)tuple.getValue(0);
    String event = (String)tuple.getValue(1);
    long ts = (Long)tuple.getValue(2);
    long date = TimeUtil.getDay(ts);
    ecu.addEvent(pid, event, date);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }
}
