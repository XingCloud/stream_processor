package com.xingcloud.stream.model;

import com.xingcloud.stream.tailer.TimeUtil;

import static com.xingcloud.stream.storm.StreamProcessorConstants.EVENT_LOG_SEPARATOR;

import java.io.Serializable;

/**
 * User: Z J Wu Date: 13-10-22 Time: 上午10:35 Package: com.xingcloud.storm.model
 */
public class StreamLogContent implements Serializable {

  private String projectId;
  private String event;
  private long date;

  public StreamLogContent(String projectId, String event, long timestamp) {
    this.projectId = projectId;
    this.event = event;
    this.date = TimeUtil.getDay(timestamp);
  }

  public String getProjectId() {
    return projectId;
  }

  public String getEvent() {
    return event;
  }

  public long getDate() {
    return date;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamLogContent)) {
      return false;
    }

    StreamLogContent that = (StreamLogContent) o;

    return event.equals(that.event) && projectId.equals(that.projectId);

  }

  @Override
  public int hashCode() {
    int result = projectId.hashCode();
    result = 31 * result + event.hashCode();
    return result;
  }

  @Override public String toString() {
    return "StreamLog#" +
      "projectId='" + projectId + '\'' +
      ", event='" + event + '\'' +
      ", date=" + date;
  }

  public static StreamLogContent build(String stringLog) {
    if (stringLog == null) {
      return null;
    }

    int defaultLogLength = 5, startPosition = 0, idx;
    String[] arr = new String[defaultLogLength];
    for (int i = 0; i < defaultLogLength; i++) {
      idx = stringLog.indexOf(EVENT_LOG_SEPARATOR, startPosition);
      arr[i] = idx < 0 ? stringLog.substring(startPosition,
              stringLog.length()) : stringLog.substring(startPosition, idx);
      startPosition = idx + 1;
    }
    return new StreamLogContent(arr[0], arr[2], Long.valueOf(arr[4]));
  }

  public static void main(String[] args) {
    String s = "qvo6\t14418611\tvisit.\t0\t1382409137451";
    System.out.println(build(s));
  }

}
