package com.big.data.plan;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Created by seven on 25/02/16.
 * kafka新api操作
 */
public class NewApiKafkaProducer {
    private static final Logger logger = Logger.getLogger(NewApiKafkaProducer.class);
    private Properties props;

    public NewApiKafkaProducer() {
        props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public void produce() {
        Producer<String, String> producer = new KafkaProducer<>(props);
        String key = null;
        String value = null;
        for (int i = 0; i < 100; i++) {
            key = Integer.toString(i);
            value = Integer.toString(i);
            producer.send(new ProducerRecord<String, String>("my-topic", key, value));

            logger.info("==========send ok, key:" + key + ",value:" + value);
        }

        producer.close();
    }

    public static void main(String[] args) {
        new NewApiKafkaProducer().produce();
    }
}
