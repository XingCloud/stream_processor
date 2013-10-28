package com.xingcloud.stream.model;

import static com.xingcloud.stream.StreamProcessorConstants.DEFAULT_UPDATE_INTERVAL;
import static com.xingcloud.stream.StreamProcessorConstants.FLUSH_QUEUE_ID;
import static com.xingcloud.stream.StreamProcessorConstants.GENERIC_SEPARATOR;

import com.xingcloud.stream.queue.RedisQueue;

import java.util.concurrent.TimeUnit;

/**
 * User: Z J Wu Date: 13-10-24 Time: 下午3:36 Package: com.xingcloud.storm.model
 */
public class EventCounterUpdater implements Runnable {
  private String projectId;
  private String event;
  private long count;
  private long sum;
  private long createTimestamp;
  private long lastTimestamp;
  private long lastFlushTimestamp;

  public EventCounterUpdater(StreamLogContent slc) {
    this.projectId = slc.getProjectId();
    this.event = slc.getEvent();
    this.count = 1l;
    this.sum = slc.getEventValue();
    this.createTimestamp = slc.getTimestamp();
    this.lastTimestamp = this.createTimestamp;
    new Thread(this).start();
  }

  public synchronized void update(long eventValue, long currentTimestamp) {
    long t1 = lastTimestamp / DEFAULT_UPDATE_INTERVAL;
    long t2 = currentTimestamp / DEFAULT_UPDATE_INTERVAL;
    if ((t2 - t1) >= 1) {
      flushAndClear(false);
    }

    ++this.count;
    this.sum += eventValue;
    this.lastTimestamp = currentTimestamp;
  }

  public synchronized boolean flushAndClear(boolean checkLastFlush) {
    if (checkLastFlush && checkDataIsInitedData() && (System.currentTimeMillis() - this.lastFlushTimestamp) < 5000) {
      return false;
    }

    RedisQueue.getInstance().offer(FLUSH_QUEUE_ID, toResultString());
    this.count = 0;
    this.sum = 0;
    this.lastTimestamp = 0;
    this.lastFlushTimestamp = System.currentTimeMillis();
    return true;
  }

  private boolean checkDataIsInitedData() {
    System.out.println(
      "last auto-flush data: " + this.count + " - " + this.sum + " - " + (this.count == 0 && this.sum == 0));
    return this.count == 0 && this.sum == 0;
  }

  private synchronized String toResultString() {
    StringBuilder sb = new StringBuilder(this.projectId);
    sb.append(GENERIC_SEPARATOR);
    sb.append(this.event);
    sb.append(GENERIC_SEPARATOR);
    sb.append(this.createTimestamp);
    sb.append(GENERIC_SEPARATOR);
    sb.append(this.count);
    sb.append(GENERIC_SEPARATOR);
    sb.append(this.sum);
    return sb.toString();
  }

  @Override
  public String toString() {
    return this.projectId + "[" + this.event + "]=(C." + this.count + ".S." + this.sum + ")|(t=" + this.createTimestamp + ",t'=" + this.lastTimestamp + ".t\"=" + this.lastFlushTimestamp + ")";
  }

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      if (flushAndClear(true)) {
        System.out.println("[COUNTER-UPDATER] - Self updated.");
      }
      try {
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
