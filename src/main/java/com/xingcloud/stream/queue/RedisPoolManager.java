package com.xingcloud.stream.queue;

import com.xingcloud.xa.conf.Config;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * User: Z J Wu Date: 13-10-22 Time: 下午4:43 Package: com.xingcloud.storm.queue
 */
public class RedisPoolManager {
  private static final Logger LOGGER = Logger.getLogger(RedisPoolManager.class);
  private static RedisPoolManager instance;

  public synchronized static RedisPoolManager getInstance() {
    if (instance == null) {
      instance = new RedisPoolManager();
    }
    return instance;
  }

  private RedisPoolManager() {
    init("/streaming.default.properties", "/streaming.default.properties");
  }

  protected JedisPool pool;

  protected void init(String defaultConfFile, String classpathFile) {
    if (pool != null) {
      return;
    }
    Configuration config = Config
      .createConfig(defaultConfFile, classpathFile, null, null, Config.ConfigFormat.properties);

    int maxActive = config.getInt("max_active", 4096);
    int maxIdle = config.getInt("max_idle", 1024);
    int timeout = config.getInt("timeout", 300000);
    int maxWait = config.getInt("max_wait", 600000);

    LOGGER.info("[REDIS-INIT] - Max active - " + maxActive);
    LOGGER.info("[REDIS-INIT] - Max idle - " + maxIdle);
    LOGGER.info("[REDIS-INIT] - Timeout - " + timeout);
    LOGGER.info("[REDIS-INIT] - Max wait - " + maxWait);

    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxActive(maxActive);
    poolConfig.setMaxIdle(maxIdle);
    poolConfig.setMaxWait(maxWait);
    poolConfig.setTestOnBorrow(true);

    String url = config.getString("url");
    String[] urlParam = url.split(":");
    LOGGER.info("[REDIS-INIT] - URL - " + url);
    pool = new JedisPool(poolConfig, urlParam[0], Integer.parseInt(urlParam[1]));
  }

  public Jedis borrowJedis() {
    return pool.getResource();
  }

  public void returnJedis(Jedis jedis) {
    if (jedis == null) {
      LOGGER.info("[REDIS-INIT] - Empty jedis, ignore return.");
      return;
    }
    pool.returnResource(jedis);
  }

  public void returnBrokenJedis(Jedis jedis) {
    if (jedis == null) {
      LOGGER.info("[REDIS-INIT] - Empty jedis, ignore return broken.");
      return;
    }
    pool.returnBrokenResource(jedis);
  }
}
