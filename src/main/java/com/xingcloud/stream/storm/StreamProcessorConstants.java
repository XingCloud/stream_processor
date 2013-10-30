package com.xingcloud.stream.storm;

/**
 * User: Z J Wu Date: 13-10-23 Time: 下午3:18 Package: com.xingcloud.storm
 */
public class StreamProcessorConstants {
  public static final String MAIN_QUEUE_ID = "STREAMING_QUEUE";
  public static final String FLUSH_QUEUE_ID = "FLUSH_RESULT_QUEUE";
  public static final char NAME_SEPARATOR = '_';
  public static final char EVENT_LOG_SEPARATOR = '\t';

  public static final String EVENT_LOG = "event_log";
  public static final String SHUFFLE_KEY = "shuffle_key";
  public static final String EVENT_NAME = "event_name";
  public static final String PID = "pid";
  public static final String TS = "ts";

  public static final String EVENT_COUNTER_DB = "user_info";
  public static final String EVENT_COUNTER_COLL = "event_count";

  public static String topoKeyword = "event_counter";
  public static String spoutName = "stream_log_tail_receiver";
  public static String shuffleBoltName = "shuffler";
  public static String historyCounterBoltName = "history_counter";
}
