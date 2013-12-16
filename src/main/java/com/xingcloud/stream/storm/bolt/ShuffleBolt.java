package com.xingcloud.stream.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.xingcloud.stream.model.StreamLogContent;
import com.xingcloud.stream.storm.StreamProcessorConstants;
import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-10-29
 * Time: 下午11:58
 */
public class ShuffleBolt extends BaseBasicBolt {
  private static Logger logger = Logger.getLogger(ShuffleBolt.class);

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    StreamLogContent log = (StreamLogContent)tuple.getValue(0);
    Values values = new Values(log.getProjectId(), log.getEvent(), log.getTimestamp());
    collector.emit(values);
    logger.debug("Emit " + log.getProjectId() + "\t" + log.getEvent() + "\t" + log.getTimestamp());
    //todo: no ack?
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(
      new Fields(
        StreamProcessorConstants.PID,
        StreamProcessorConstants.EVENT_NAME,
        StreamProcessorConstants.TS
      )
    );
  }
}
