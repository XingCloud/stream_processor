package com.xingcloud.stream.queue;

import com.xingcloud.stream.model.StreamLogContent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-10-28
 * Time: 下午5:11
 * To change this template use File | Settings | File Templates.
 */
public class NativeQueue {
  private static Log LOG = LogFactory.getLog(NativeQueue.class);

  private Queue<StreamLogContent> queue = new LinkedList();

  private static NativeQueue nativeQueue = new NativeQueue();

  private NativeQueue() {

  }

  public static NativeQueue getInstance() {
    return nativeQueue;
  }

  public synchronized boolean add(StreamLogContent log) {
    return queue.add(log);
  }

  public synchronized boolean addAll(List<StreamLogContent> logs) {
    return queue.addAll(logs);
  }

  public synchronized StreamLogContent poll() {
    return queue.poll();
  }

  public synchronized int size() {
    return queue.size();
  }

  public synchronized StreamLogContent peek() {
    return queue.peek();
  }


}
