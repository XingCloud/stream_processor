package com.xingcloud.stream.storm;

/**
 * User: Z J Wu Date: 13-10-23 Time: 下午3:18 Package: com.xingcloud.storm
 */
public class StreamProcessorConstants {
  public static final char NAME_SEPARATOR = '_';
  public static final char EVENT_LOG_SEPARATOR = '\t';

  public static final String EVENT_LOG = "event_log";
  public static final String EVENT_NAME = "event_name";
  public static final String PID = "pid";
  public static final String TS = "ts";
  public static final String DATE = "date";

  public static final String EVENT_COUNTER_COLL = "event_count";

  public static final String topoKeyword = "event_counter";
  public static final String spoutName = "stream_log_tail_receiver";
  public static final String shuffleBoltName = "shuffler";
  public static final String historyCounterBoltName = "history_counter";
}
