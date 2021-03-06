package com.xingcloud.stream.model;


import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.xingcloud.mongo.MongoDBManager;
import com.xingcloud.stream.storm.StreamProcessorConstants;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EventCounterUpdater implements Runnable, Serializable{
  private static Logger logger = Logger.getLogger(EventCounterUpdater.class);

  private static final int FLUSH_KEY_SIZE = 2000;
  private static final long FLUSH_INTERVAL = 5 * 60 * 1000;
  private static final long SLEEP_INTERVAL = 1000;

  private long totalEventNum = 0l;
  private long lastFlushTime = System.currentTimeMillis();
  // project id -> (date -> (event -> count))
  private Map<String, Map<Long, Map<String, Long>>> eventCounterMap = new HashMap<String, Map<Long, Map<String, Long>>>();
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public EventCounterUpdater() {
  }

  public long addEvent(String pid, String event, long date) {
    long current = 0;
    try {
      lock.writeLock().lock();
      Map<Long, Map<String, Long>> eachDateMap = eventCounterMap.get(pid);
      if (null == eachDateMap) {
        eachDateMap = new HashMap<Long, Map<String, Long>>();
        Map<String, Long> eachEventMap = new HashMap<String, Long>();
        eachEventMap.put(event, 1l);
        eachDateMap.put(date, eachEventMap);
        eventCounterMap.put(pid, eachDateMap);
      } else {
        Map<String, Long> eachEventMap = eachDateMap.get(date);
        if (null == eachEventMap) {
          eachEventMap = new HashMap<String, Long>();
          eachEventMap.put(event, 1l);
          eachDateMap.put(date, eachEventMap);
        } else {
          Long count = eachEventMap.get(event);
          if (null == count) {
            eachEventMap.put(event, 1l);
          } else {
            eachEventMap.put(event, 1l+count);
          }
        }
      }
      totalEventNum++;
      current = totalEventNum;
      logger.debug("Current event number: " + current);
    } finally {
      lock.writeLock().unlock();
    }
    return current;
  }

  public void flushToMongo() {
    try {
      lock.writeLock().lock();
      logger.info("--------- Start to update mongodb. Current event number: " + totalEventNum + " ---------");
      long st = System.nanoTime();
      DBCollection coll = MongoDBManager.getInstance().getDB()
              .getCollection(StreamProcessorConstants.EVENT_COUNTER_COLL);

      for (Map.Entry<String, Map<Long, Map<String, Long>>> entry : eventCounterMap.entrySet()) {
        String pid = entry.getKey();
        Map<Long, Map<String, Long>> eachDateMap = entry.getValue();
        for (Map.Entry<Long, Map<String, Long>> subEntry : eachDateMap.entrySet()) {
          long date = subEntry.getKey();
          Map<String, Long> eachEventMap = subEntry.getValue();
          for (Map.Entry<String, Long> eventEntry : eachEventMap.entrySet()) {
            String event = eventEntry.getKey();
            long count = eventEntry.getValue();
            updateMongo(pid, date, event, count, coll);
          }
        }
      }
      cleanUp();
      logger.info("Update event count value to MongoDB finish. Taken: " + (System.nanoTime()-st)/1.0e9 + " sec");
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void cleanUp() {
    totalEventNum = 0;
    eventCounterMap.clear();
  }

  private void updateMongo(String pid, long date, String event, long count, DBCollection coll) {
    BasicDBObject searchQuery = getSearchQuery(pid, event, date);
    BasicDBObject updateQuery = getUpdateQuery(count);
    logger.debug("Update json: " + searchQuery + "\t" + updateQuery);
    coll.update(searchQuery, updateQuery, true, false);
  }

  private BasicDBObject getSearchQuery(String pid, String event, long date) {
    String[] fields = event.split("\\.");
    BasicDBObject searchQuery = new BasicDBObject();
    for (int i=0; i<fields.length; i++) {
      searchQuery.put("l"+i, fields[i]);
    }
    //todo: 查询条件不准确
    searchQuery.put("date", date);
    searchQuery.put("project_id", pid);
    return searchQuery;
  }

  private BasicDBObject getUpdateQuery(long count) {
    BasicDBObject updateQuery = new BasicDBObject();
    updateQuery.put("$inc", new BasicDBObject().append("count", count));
    return updateQuery;
  }

  private void printMap() {
    StringBuilder summary = new StringBuilder("--- Summary:\n");
    for (Map.Entry<String, Map<Long, Map<String, Long>>> entry : eventCounterMap.entrySet()) {
      String pid = entry.getKey();
      summary.append("PID: ").append(pid).append(":\n");
      Map<Long, Map<String, Long>> eachDateMap = entry.getValue();
      for (Map.Entry<Long, Map<String, Long>> subEntry : eachDateMap.entrySet()) {
        long date = subEntry.getKey();
        summary.append("Date: ").append(date).append(":\n");
        Map<String, Long> eachEventMap = subEntry.getValue();
        for (Map.Entry<String, Long> eventEntry : eachEventMap.entrySet()) {
          String event = eventEntry.getKey();
          long count = eventEntry.getValue();
          summary.append(event).append(": ").append(count).append("\n");
        }
      }
    }
    logger.info(summary.toString());
  }

  @Override
  public void run() {
    logger.info("Start mongodb update thread " + Thread.currentThread().getName());
    while (true) {
        if (((System.currentTimeMillis()-lastFlushTime)>FLUSH_INTERVAL && eventCounterMap.size()!=0) ||
                (totalEventNum > FLUSH_KEY_SIZE)) {
          flushToMongo();
          lastFlushTime = System.currentTimeMillis();
        }
        try {
          Thread.sleep(SLEEP_INTERVAL);
        } catch (InterruptedException e) {
          e.printStackTrace();
          logger.error(e);
        }
    }
  }



}
