package com.xingcloud.stream.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.xingcloud.stream.model.EventCounterUpdater;
import com.xingcloud.stream.storm.StreamProcessorConstants;
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
    super.prepare(stormConf, context);
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String pid = (String)tuple.getValue(0);
    String event = (String)tuple.getValue(1);
    long ts = (Long)tuple.getValue(2);
    long date = TimeUtil.getDay(ts);
    ecu.addEvent(pid, event, date);
    Values values = new Values(pid, event, date);
    collector.emit(values);
    logger.debug("Update count " + pid + "\t" + event + "\t" + date);

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields(StreamProcessorConstants.PID,
            StreamProcessorConstants.EVENT_NAME, StreamProcessorConstants.DATE));
  }
}
