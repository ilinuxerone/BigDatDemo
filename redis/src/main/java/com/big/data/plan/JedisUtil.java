package com.big.data.plan;

import redis.clients.jedis.JedisPool;

public class JedisUtil {

    private static JedisPool pool;

    static JedisPool getPool() {
        if (pool == null) {
            pool = new JedisPool(getRedisAddress(), getRedisPort());
        }
        return pool;
    }

    public static String getRedisAddress() {
        return "127.0.0.1";
    }

    public static int getRedisPort() {
        return 6379;
    }
}
