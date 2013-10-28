package com.xingcloud.stream.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.base.BaseRichBolt;
import com.xingcloud.stream.model.EventCounterUpdater;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Z J Wu Date: 13-10-24 Time: 下午1:53 Package: com.xingcloud.storm.bolt
 */
public abstract class BaseEventCountBolt extends BaseRichBolt {
  protected Map<String, EventCounterUpdater> counter = new HashMap<String, EventCounterUpdater>();
  protected OutputCollector collector;
}
