package com.xingcloud.stream.tailer;

import com.xingcloud.stream.model.StreamLogContent;
import com.xingcloud.stream.queue.NativeQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-10-28
 * Time: 下午5:04
 * To change this template use File | Settings | File Templates.
 */
public class StreamLogTailer extends Tail{
  private static Log LOG = LogFactory.getLog(StreamLogTailer.class);

  public static final String configPath = "/data/log/stream_config/";
  private int MAX_SIZE = 20000;
  private long SLEEP_INTERVAL = 1000;

  public StreamLogTailer(String configPath) {
    super(configPath);
  }

  @Override
  public void send(List<String> logs, long day) {
    LOG.info("Tail stream log size: " + logs.size() + "\tDay: " + day);
    List<StreamLogContent> events = new ArrayList<StreamLogContent>();
    for (String line : logs) {
      try {
        StreamLogContent log = StreamLogContent.build(line);
        events.add(log);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    NativeQueue.getInstance().addAll(events);
    while (NativeQueue.getInstance().size() > MAX_SIZE) {
      LOG.info("Queue size(" + NativeQueue.getInstance().size() + ") is greater than " + MAX_SIZE + ", sleep for " + SLEEP_INTERVAL + " ms");
      try {
        Thread.sleep(SLEEP_INTERVAL);
      } catch (InterruptedException e) {
        e.printStackTrace();
        LOG.error(e);
      }
    }

  }
}
