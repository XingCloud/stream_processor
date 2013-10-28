package com.xingcloud.stream.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.xingcloud.stream.model.StreamLogContent;
import com.xingcloud.stream.queue.RedisQueue;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * User: Z J Wu Date: 13-10-22 Time: 上午9:52 Package: com.xingcloud.storm.spout
 */
public class StreamLogReadSpout extends BaseRichSpout {

  private static final Logger LOGGER = Logger.getLogger(StreamLogReadSpout.class);

  private RedisQueue redisQueue;

  private SpoutOutputCollector collector;

  private boolean debug;

  private boolean read = false;

  private String id;

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("line"));
  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    this.redisQueue = RedisQueue.getInstance();
    this.collector = spoutOutputCollector;
    Object o = map.get("debug");
    if (o == null) {
      this.debug = false;
    } else {
      this.debug = Boolean.valueOf(o.toString());
    }
    this.id = UUID.randomUUID().toString();
    LOGGER.info("[SPOUT] - Spout inited(" + this.id + ").");
  }

  @Override
  public void nextTuple() {
    if (debug) {
      nextTupleDebug();
    } else {
      _nextTuple();
    }
//    try {
//      TimeUnit.MILLISECONDS.sleep(100);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
  }

  private void nextTupleDebug() {
    if (read) {
      return;
    }
    read = true;
    File f = new File("/test.log");
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(f));
      String line;
      StreamLogContent slc;
      while ((line = br.readLine()) != null) {
        slc = StreamLogContent.build(line);
        collector.emit(new Values(slc), line);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        br.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void _nextTuple() {
    String log = StringUtils.trimToNull(redisQueue.poll());
    StreamLogContent slc = StreamLogContent.build(log);
    if (log != null) {
      collector.emit(new Values(slc), log);
    }
  }

  @Override
  public void fail(Object msgId) {
//    redisQueue.offer(msgId.toString());
    LOGGER.error("[SPOUT] Message failed, put it to queue again.");
  }

  @Override public void ack(Object msgId) {
//    LOGGER.info("[SPOUT] Message ack(" + this.id + ")");
  }
}

