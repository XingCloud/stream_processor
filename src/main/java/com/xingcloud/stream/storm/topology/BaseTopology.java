package com.xingcloud.stream.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;

import java.util.concurrent.TimeUnit;

/**
 * User: Z J Wu Date: 13-10-21 Time: 下午1:56 Package: com.xingcloud.storm
 */
public abstract class BaseTopology {

  protected static void runTopologyLocal(String topoId, Config config, StormTopology topology, long sleep) throws
    InterruptedException {
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topoId, config, topology);
    if (sleep > 0) {
      TimeUnit.SECONDS.sleep(sleep);
      cluster.shutdown();
    }
  }
}
