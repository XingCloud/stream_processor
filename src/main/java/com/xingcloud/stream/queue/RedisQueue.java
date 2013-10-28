package com.xingcloud.stream.queue;

import static com.xingcloud.stream.StreamProcessorConstants.MAIN_QUEUE_ID;

import com.xingcloud.stream.exception.StreamProcessRuntimeException;
import redis.clients.jedis.Jedis;

/**
 * User: Z J Wu Date: 13-10-22 Time: 下午4:41 Package: com.xingcloud.storm.queue
 */
public class RedisQueue {
  private static RedisQueue instance;

  public synchronized static RedisQueue getInstance() {
    if (instance == null) {
      instance = new RedisQueue();
    }
    return instance;
  }

  private RedisQueue() {
  }

  private final RedisPoolManager manager = RedisPoolManager.getInstance();

  public String poll() throws StreamProcessRuntimeException {
    return poll(MAIN_QUEUE_ID);
  }

  public String poll(String queueId) throws StreamProcessRuntimeException {
    Jedis jedis = null;
    boolean successful = true;
    try {
      jedis = manager.borrowJedis();
      return jedis.lpop(queueId);
    } catch (Exception e) {
      e.printStackTrace();
      successful = false;
      throw new StreamProcessRuntimeException("Error occurred when polling from redis.", e);
    } finally {
      if (successful) {
        manager.returnJedis(jedis);
      } else {
        manager.returnBrokenJedis(jedis);
      }
    }
  }

  public void offer(String s) throws StreamProcessRuntimeException {
    offer(MAIN_QUEUE_ID, s);
  }

  public void offer(String queueId, String s) throws StreamProcessRuntimeException {
    Jedis jedis = null;
    boolean successful = true;
    try {
      jedis = manager.borrowJedis();
      successful = true;
      jedis.rpush(queueId, s);
    } catch (Exception e) {
      successful = false;
      throw new StreamProcessRuntimeException("Error occurred when offering from redis.", e);
    } finally {
      if (successful) {
        manager.returnJedis(jedis);
      } else {
        manager.returnBrokenJedis(jedis);
      }
    }
  }

}
