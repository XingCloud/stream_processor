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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.UUID;

/**
 * User: Z J Wu Date: 13-10-22 Time: 上午9:52 Package: com.xingcloud.storm.spout
 */
public class StreamLogReadSpout extends BaseRichSpout {

  private static final Log LOG = LogFactory.getLog(StreamLogReadSpout.class);

  private SpoutOutputCollector _collector;

  private String id;

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields(StreamProcessorConstants.EVENT_LOG));
  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    this._collector = spoutOutputCollector;
    this.id = UUID.randomUUID().toString();
    LOG.info("[SPOUT] - Spout inited(" + this.id + ").");
  }

  @Override
  public void nextTuple() {
    StreamLogContent log = NativeQueue.getInstance().poll();
    if (log != null) {
      Values values = new Values(log);
      _collector.emit(values);
    }
  }

  @Override
  public void fail(Object msgId) {
    LOG.error("[SPOUT] Message failed, put it to queue again.");
  }


}

