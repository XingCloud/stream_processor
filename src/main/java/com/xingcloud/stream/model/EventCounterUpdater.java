package com.xingcloud.stream.model;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.xingcloud.mongo.MongoDBManager;
import com.xingcloud.stream.storm.StreamProcessorConstants;
import com.xingcloud.stream.utils.Tuple3;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class EventCounterUpdater implements Runnable, Serializable{
  private static Logger logger = Logger.getLogger(EventCounterUpdater.class);

  private static final int FLUSH_KEY_SIZE = 5000;
  private static final long FLUSH_INTERVAL = 5 * 60 * 1000;
  private static final long SLEEP_INTERVAL = 2000;

  private long lastFlushTime = System.currentTimeMillis();

  //(project id, date, event) -> count
  private Map<Tuple3<String , Long, String>, Long> eventCounterMap =
    new HashMap<Tuple3<String, Long, String>, Long>();

  public EventCounterUpdater() {
  }

  public synchronized void addEvent(String pid, String event, long date) {
    Tuple3<String, Long, String> tuple3 = new Tuple3<String, Long, String>(pid, date, event);
    if (!eventCounterMap.containsKey(tuple3)) {
      eventCounterMap.put(tuple3, 1L);
    } else {
      eventCounterMap.put(tuple3, eventCounterMap.get(tuple3) + 1L);
    }
  }

  private synchronized void flushToMongo() {
    logger.info("--------- Start to update mongodb. Current map size: " + eventCounterMap.size() + " ---------");

    long start = System.currentTimeMillis();
    DBCollection coll = MongoDBManager.getInstance()
      .getDB().getCollection(StreamProcessorConstants.EVENT_COUNTER_COLL);

    for (Map.Entry<Tuple3<String, Long, String>, Long> entry : eventCounterMap.entrySet()) {
      Tuple3<String, Long, String> tuple3 = entry.getKey();
      updateMongo(tuple3.first, tuple3.second, tuple3.third, entry.getValue(), coll);
    }

    logger.info("Update event count value to MongoDB finish. Taken: " + (System.currentTimeMillis() - start) + "ms.");

    eventCounterMap.clear();
    lastFlushTime = System.currentTimeMillis();
  }

  private void updateMongo(String pid, long date, String event, long count, DBCollection coll) {
    BasicDBObject searchQuery = getSearchQuery(pid, event, date);
    BasicDBObject updateQuery = getUpdateQuery(count);
//    logger.debug("Update json: " + searchQuery + "\t" + updateQuery);
    coll.update(searchQuery, updateQuery, true, false);
  }

  private BasicDBObject getSearchQuery(String pid, String event, long date) {
    String[] fields = event.split("\\.");

    BasicDBObject query = new BasicDBObject();
    query.append("project_id", pid);
    query.append("date", date);
    for (int i = 0; i < fields.length; i++) {
      query.append("l" + i, fields[i]);
    }
    //最多6层事件
    for (int i = fields.length; i < 6; i++) {
      query.append("l" + i, new BasicDBObject("$exists", false));
    }

    return query;
  }

  private BasicDBObject getUpdateQuery(long count) {
    BasicDBObject updateQuery = new BasicDBObject();
    updateQuery.put("$inc", new BasicDBObject().append("count", count));
    return updateQuery;
  }

  private synchronized int getMapSize() {
    return eventCounterMap.size();
  }

  private synchronized void printMap() {
    StringBuilder summary = new StringBuilder("--- Summary:\n");
    for (Map.Entry<Tuple3<String, Long, String>, Long> entry : eventCounterMap.entrySet()) {
      Tuple3<String, Long, String> tuple3 = entry.getKey();
      summary.append("PID: ").append(tuple3.first).append(":\n");
      summary.append("Date: ").append(tuple3.second).append(":\n");
      summary.append(tuple3.third).append(": ").append(entry.getValue()).append("\n");
    }

    logger.info(summary.toString());
  }

  @Override
  public void run() {
    logger.info("Start mongodb update thread " + Thread.currentThread().getName());

    while (true) {
      int size = getMapSize();
      long current = System.currentTimeMillis();
      if (((current - lastFlushTime) > FLUSH_INTERVAL && size > 0) || size > FLUSH_KEY_SIZE) {
        flushToMongo();
      }

      try {
        Thread.sleep(SLEEP_INTERVAL);
      } catch (InterruptedException e) {
        logger.error(e.getMessage(), e);
      }
    }
  }

  public static void main(String... args) {
    String event = "a.b.c.d.";
    String[] segments = event.split("\\.");
    for (String s : segments) {
      System.out.println(">>>>> " + s);
    }

    EventCounterUpdater ecu = new EventCounterUpdater();
    System.out.println(ecu.getSearchQuery("test", event, 20131219L));
  }
}
