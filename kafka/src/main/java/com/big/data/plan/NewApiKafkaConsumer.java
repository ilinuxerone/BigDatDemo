package com.big.data.plan;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by seven on 25/02/16.
 * kafka新api操作
 */
public class NewApiKafkaConsumer {
    private static final Logger logger = Logger.getLogger(NewApiKafkaConsumer.class);
    private Properties props = null;

    public NewApiKafkaConsumer() {
        props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    }

    public void consumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("my-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                logger.info("offset:" + record.offset() + ",key:" + record.key() + ",value:"
                            + record.value());
        }
    }

    public static void main(String[] args) {
        new NewApiKafkaConsumer().consumer();
    }
}
