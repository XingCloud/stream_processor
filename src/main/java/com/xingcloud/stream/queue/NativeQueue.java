package com.xingcloud.stream.queue;

import com.xingcloud.stream.model.StreamLogContent;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-10-28
 * Time: 下午5:11
 */
public class NativeQueue {

  private Queue<StreamLogContent> queue = new LinkedList<StreamLogContent>();

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
