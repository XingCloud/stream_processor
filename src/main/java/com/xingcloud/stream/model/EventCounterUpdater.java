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

  private static final int FLUSH_KEY_SIZE = 10000;
  private static final long FLUSH_INTERVAL = 5 * 60 * 1000;
  private static final long SLEEP_INTERVAL = 1000;

  private long totalEventNum = 0l;
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

    totalEventNum++;
  }

  private synchronized void flushToMongo() {
    logger.info("--------- Start to update mongodb. Current event number: " + totalEventNum + " ---------");

    long start = System.currentTimeMillis();
    DBCollection coll = MongoDBManager.getInstance()
      .getDB().getCollection(StreamProcessorConstants.EVENT_COUNTER_COLL);

    for (Map.Entry<Tuple3<String, Long, String>, Long> entry : eventCounterMap.entrySet()) {
      Tuple3<String, Long, String> tuple3 = entry.getKey();
      updateMongo(tuple3.first, tuple3.second, tuple3.third, entry.getValue(), coll);
    }

    logger.info("Update event count value to MongoDB finish. Taken: " + (System.currentTimeMillis() - start) + "ms.");

    cleanUp();
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
