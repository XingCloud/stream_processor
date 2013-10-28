package com.xingcloud.stream.queue;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Random;

/**
 * User: Z J Wu Date: 13-10-25 Time: 下午3:32 Package: com.xingcloud.storm.queue
 */
public class QueuePutter {
  public static void main(String[] args) throws InterruptedException {
    String str = ArrayUtils.isEmpty(args) ? "0" : args[0];
    int times = Integer.valueOf(str);

    Object[][] eventPool = new Object[][]{new Object[]{"visit.", 0}, new Object[]{"pay.", 0},
                                          new Object[]{"heartbeat.", 0}
    };
    eventPool = new Object[][]{new Object[]{"visit.", 0}};
    String projectId = "qvo6";

    String log;
    char sp = ' ';
    Random r = new Random();

    Object[] obj;

    if (times == 0) {
      while (true) {
        obj = eventPool[r.nextInt(eventPool.length)];
        log = projectId + sp + "123" + sp + obj[0] + sp + 0 + sp + System.currentTimeMillis();
        RedisQueue.getInstance().offer(log);
        System.out.println("Put queue: " + log);
        Integer cnt = (Integer) obj[1];
        ++cnt;
        obj[1] = cnt;
      }
    } else {
      for (int i = 0; i < times; i++) {
        obj = eventPool[r.nextInt(eventPool.length)];
        log = projectId + sp + "123" + sp + obj[0] + sp + 0 + sp + System.currentTimeMillis();
        RedisQueue.getInstance().offer(log);
        System.out.println("Put queue: " + log);
        Integer cnt = (Integer) obj[1];
        ++cnt;
        obj[1] = cnt;
      }
    }
    System.out.println("---------------------------------");
    for (Object[] o : eventPool) {
      System.out.println(o[0] + " - " + o[1]);
    }
    System.out.println("---------------------------------");
  }
}
