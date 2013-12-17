package com.xingcloud.stream.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.xingcloud.stream.model.StreamLogContent;
import com.xingcloud.stream.queue.NativeQueue;
import com.xingcloud.stream.storm.StreamProcessorConstants;
import com.xingcloud.stream.tailer.StreamLogTailer;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * User: Z J Wu Date: 13-10-22 Time: 上午9:52 Package: com.xingcloud.storm.spout
 */
public class StreamLogReadSpout extends BaseRichSpout {

  private static final Logger logger = Logger.getLogger(StreamLogReadSpout.class);

  private SpoutOutputCollector _collector;

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

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    this._collector = spoutOutputCollector;

    Thread tailerThread = new Thread() {
      @Override
      public synchronized void run() {
        StreamLogTailer streamLogTailer = new StreamLogTailer(StreamLogTailer.configPath);
        try {
          streamLogTailer.start();
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
        }
      }
    };

    tailerThread.start();
  }

  @Override
  public void nextTuple() {
    StreamLogContent log = NativeQueue.getInstance().poll();
    if (log != null) {
      Values values = new Values(log.getProjectId(), log.getEvent(), log.getTimestamp());
      _collector.emit(values);
    }
  }

}

