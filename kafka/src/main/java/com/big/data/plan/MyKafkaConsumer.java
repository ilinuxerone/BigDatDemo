package com.big.data.plan;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by seven on 23/02/16.
 * kafka消费线程
 */
public class MyKafkaConsumer {
    private static Logger LOG = Logger.getLogger(MyKafkaConsumer.class);

    private ConsumerConnector consumer;

    private int flushCountTag = 1;
    private int flushCount = Integer.valueOf(Constant.FLUSH_COUNT);

    /**
     * 读取配置:实例化一个consumer
     */
    private MyKafkaConsumer() {
        Properties props = new Properties();
        props.put("zookeeper.connect", Constant.ZOOKEEPER);

        props.put("group.id", "plus-test-local");

        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "1000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "largest");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("auto.commit.enable", "false");

        ConsumerConfig config = new ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    void consume() {
        String topicName = Constant.TOPIC;

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topicName, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer
            .createMessageStreams(topicCountMap, keyDecoder, valueDecoder);

        KafkaStream<String, String> stream = consumerMap.get(topicName).get(0);
        ConsumerIterator<String, String> it = stream.iterator();

        while (it.hasNext()) {
            if (flushCountTag % flushCount != 0) {

                String message = it.next().message();

                LOG.info("============>MyKafkaConsumer receive:" + message);

                consumer.commitOffsets();
                flushCountTag++;

            } else {

                flushCountTag = 1;
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage());
                }

            }
        }
    }

    /**
     * 1.开启线程将消息反序列化、将队列里的数据写入数据库
     * 2.将消息写入本地日志、并写入队列里
     *
     * @param args
     */
    public static void main(String[] args) {
        LOG.info("=====>MyKafkaConsumer");
        new MyKafkaConsumer().consume();
    }
}
