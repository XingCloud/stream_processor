package com.xingcloud.stream;

/**
 * User: Z J Wu Date: 13-10-23 Time: 下午3:18 Package: com.xingcloud.storm
 */
public class StreamProcessorConstants {
  public static final String MAIN_QUEUE_ID = "STREAMING_QUEUE";
  public static final String FLUSH_QUEUE_ID = "FLUSH_RESULT_QUEUE";
  public static final char NAME_SEPARATOR = '_';
  public static final char EVENT_LOG_SEPARATOR = ' ';
  public static final char GENERIC_SEPARATOR = '|';

  public static final int DEFAULT_UPDATE_INTERVAL = 1000;
}
