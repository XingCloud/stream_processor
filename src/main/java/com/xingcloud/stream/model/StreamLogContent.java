package com.xingcloud.stream.model;

import static com.xingcloud.stream.StreamProcessorConstants.EVENT_LOG_SEPARATOR;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * User: Z J Wu Date: 13-10-22 Time: 上午10:35 Package: com.xingcloud.storm.model
 */
public class StreamLogContent implements Serializable {

  private String projectId;
  private String originalId;
  private String event;
  private long eventValue;
  private long timestamp;

  public StreamLogContent(String projectId, String event, long eventValue, long timestamp) {
    this.projectId = projectId;
    this.event = event;
    this.eventValue = eventValue;
    this.timestamp = timestamp;
  }

  public StreamLogContent(String projectId, String originalId, String event, long eventValue, long timestamp) {
    this.projectId = projectId;
    this.originalId = originalId;
    this.event = event;
    this.eventValue = eventValue;
    this.timestamp = timestamp;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getOriginalId() {
    return originalId;
  }

  public void setOriginalId(String originalId) {
    this.originalId = originalId;
  }

  public String getEvent() {
    return event;
  }

  public void setEvent(String event) {
    this.event = event;
  }

  public long getEventValue() {
    return eventValue;
  }

  public void setEventValue(long eventValue) {
    this.eventValue = eventValue;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
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

    if (!event.equals(that.event)) {
      return false;
    }
    if (!projectId.equals(that.projectId)) {
      return false;
    }

    return true;
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
      ", originalId='" + originalId + '\'' +
      ", event='" + event + '\'' +
      ", eventValue=" + eventValue +
      ", timestamp=" + timestamp;
  }

  public static StreamLogContent build(String stringLog) {
    String s = StringUtils.trimToNull(stringLog);
    if (StringUtils.isBlank(s)) {
      return null;
    }
    int defaultLogLength = 5, startPosition = 0, idx;
    String[] arr = new String[defaultLogLength];
    for (int i = 0; i < defaultLogLength; i++) {
      idx = s.indexOf(EVENT_LOG_SEPARATOR, startPosition);
      arr[i] = idx < 0 ? s.substring(startPosition, s.length()) : s.substring(startPosition, idx);
      startPosition = idx + 1;
    }
    return new StreamLogContent(arr[0], arr[2], Long.valueOf(arr[3]), Long.valueOf(arr[4]));
  }

  public static boolean isStremLogContent(Object obj) {
    return obj instanceof StreamLogContent;
  }

  public String toKey() {
    return this.projectId + '|' + this.event;
  }

  public static void main(String[] args) {
    String s = "qvo6\t14418611\tvisit.\t0\t1382409137451";
    System.out.println(build(s));
  }

}
