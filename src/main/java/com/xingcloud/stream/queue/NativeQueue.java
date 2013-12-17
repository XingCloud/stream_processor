package com.xingcloud.stream.queue;

import com.xingcloud.stream.model.StreamLogContent;

import java.util.ArrayDeque;
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

  //ArrayDeque is likely to be fast than LinkedList
  private Queue<StreamLogContent> queue = new ArrayDeque<StreamLogContent>();

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

  public static void main(String... args) {
    Queue<StreamLogContent> q1 = new LinkedList<StreamLogContent>();
    Queue<StreamLogContent> q2 = new ArrayDeque<StreamLogContent>();

    long times = 100000;

    long start = System.currentTimeMillis();
    for (int i = 0; i < times; i++) {
      q1.add(new StreamLogContent("test", "a.b.c.d.", start));
    }
    System.out.println(System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    for (int i = 0; i < times; i++) {
      q1.poll();
    }
    System.out.println(System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    for (int i = 0; i < times; i++) {
      q2.add(new StreamLogContent("test", "a.b.c.d.", start));
    }
    System.out.println(System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    for (int i = 0; i < times; i++) {
      q2.poll();
    }
    System.out.println(System.currentTimeMillis() - start);
  }

}
