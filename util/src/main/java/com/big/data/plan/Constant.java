package com.big.data.plan;

import java.text.SimpleDateFormat;

/**
 * Created by seven on 23/02/16.
 */
public class Constant {
    public static final String TOPIC = "my-replicated-topic5";
    public static final String BROKERS = "127.0.0.1:9092";
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat PRODUCE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    public static final int FLUSH_COUNT = 10;
    public static final String ZOOKEEPER = "127.0.0.1:2181";
    public static final String ZK_ROOT = "/home/seven/big-data-plan/zookeeper-3.5.1-alpha";
}
