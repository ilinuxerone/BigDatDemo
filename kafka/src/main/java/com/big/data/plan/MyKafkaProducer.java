package com.big.data.plan;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.Properties;

/**
 * Created by seven on 23/02/16.
 * kafka生产线程
 */
public class MyKafkaProducer {
    private final Producer<String, String> producer;
    public final static String TOPIC = Constant.TOPIC;
    private static Logger LOG = Logger.getLogger(MyKafkaProducer.class);

    private MyKafkaProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", Constant.BROKERS);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "-1");
        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    void produce() throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
            String now = Constant.DATE_FORMAT.format(new Date());
            String line = "hello s, i am hungry, can you gei me some food, thanks, thanks.";
            producer.send(new KeyedMessage<String, String>(TOPIC, line));
            LOG.info("============>MyKafkaProducer send:" + line);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        LOG.info("=====>MyKafkaProducer");
        new MyKafkaProducer().produce();
    }
}
