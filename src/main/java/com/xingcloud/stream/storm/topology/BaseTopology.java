package com.xingcloud.stream.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * User: Z J Wu Date: 13-10-21 Time: 下午1:56 Package: com.xingcloud.storm
 */
public abstract class BaseTopology {
  private static Log LOG = LogFactory.getLog(BaseTopology.class);

  protected static void runTopologyLocal(String topoId, Config config, StormTopology topology) throws
    InterruptedException {
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topoId, config, topology);
    Utils.sleep(1200000);
    cluster.shutdown();
    LOG.info("CLuster was killed...");
    cluster.shutdown();
  }
}
