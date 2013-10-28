package com.xingcloud.stream;

import static com.xingcloud.stream.StreamProcessorConstants.NAME_SEPARATOR;

/**
 * User: Z J Wu Date: 13-10-23 Time: 下午3:59 Package: com.xingcloud.storm
 */
public class StreamProcessorUtils {
  public static String toSpoutId(String topoName, String spoutName) {
    return "spout" + NAME_SEPARATOR + topoName + NAME_SEPARATOR + spoutName;
  }

  public static String toBoltId(String topoName, String boltName) {
    return "bolt" + NAME_SEPARATOR + topoName + NAME_SEPARATOR + boltName;
  }

  public static String toTopologyId(String name) {
    return "topology" + NAME_SEPARATOR + name;
  }
}
