package com.xingcloud.stream.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.xingcloud.stream.model.EventCounterUpdater;
import com.xingcloud.stream.model.StreamLogContent;
import com.xingcloud.stream.tailer.TimeUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;


public class EventCountHistoryBolt extends BaseBasicBolt {
  private static final Log LOG = LogFactory.getLog(EventCountHistoryBolt.class);

  private EventCounterUpdater ecu = new EventCounterUpdater();

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    Thread ecuThread = new Thread(ecu);
    ecuThread.start();
    LOG.info("EventCountHistoryBolt init bolt finish!");
    super.prepare(stormConf, context);
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String pid = (String)tuple.getValue(0);
    String event = (String)tuple.getValue(1);
    long ts = (Long)tuple.getValue(2);
    long date = TimeUtil.getDate(ts);
    ecu.addEvent(pid, event, date);
    LOG.debug("Update count " + pid + "\t" + event + "\t" + date);

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    //No output
  }
}
