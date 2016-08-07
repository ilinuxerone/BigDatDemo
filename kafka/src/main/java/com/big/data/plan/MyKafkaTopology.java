package com.big.data.plan;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.log4j.Logger;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * kafka storm
 */
public class MyKafkaTopology {

    private static final Logger LOG = Logger.getLogger(KafkaWordSplitter.class);

    public static class KafkaWordSplitter extends BaseRichBolt {
        private static final long serialVersionUID = 886149197481637894L;
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String line = input.getString(0);
            LOG.info("=========>KafkaWordSplitter RECV[kafka -> splitter] " + line);
            String[] words = line.split("\\s+");
            for (String word : words) {
                LOG.info("=========>KafkaWordSplitter EMIT[splitter -> counter] " + word);
                collector.emit(input, new Values(word, 1));
            }
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

    }

    public static class WordCounter extends BaseRichBolt {

        private static final Log LOG = LogFactory.getLog(WordCounter.class);
        private static final long serialVersionUID = 886149197481637894L;
        private OutputCollector collector;
        private Map<String, AtomicInteger> counterMap;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            this.counterMap = new HashMap<String, AtomicInteger>();
        }

        @Override
        public void execute(Tuple input) {
            String word = input.getString(0);
            int count = input.getInteger(1);
            LOG.info("==========>WordCounter RECV[splitter -> counter] " + word + " : " + count);
            AtomicInteger ai = this.counterMap.get(word);
            if (ai == null) {
                ai = new AtomicInteger();
                this.counterMap.put(word, ai);
            }
            ai.addAndGet(count);
            collector.ack(input);
            LOG.info("============>WordCounter CHECK statistics map: " + this.counterMap);
        }

        @Override
        public void cleanup() {
            LOG.info("=========>WordCounter The final result:");
            Iterator<Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
            while (iter.hasNext()) {
                Entry<String, AtomicInteger> entry = iter.next();
                LOG.info(entry.getKey() + "\t:\t" + entry.getValue().get());
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException,
                                           InterruptedException {
        LOG.info("============>MyKafkaTopology");
        String id = "word";
        BrokerHosts brokerHosts = new ZkHosts(Constant.ZOOKEEPER);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, Constant.TOPIC, Constant.ZK_ROOT, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = false;
        spoutConf.zkServers = Arrays.asList(new String[] { "127.0.0.1" });
        spoutConf.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5);
        builder.setBolt("word-splitter", new KafkaWordSplitter(), 2)
            .shuffleGrouping("kafka-reader");
        builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-splitter",
            new Fields("word"));

        Config conf = new Config();

        String name = MyKafkaTopology.class.getSimpleName();
        if (args != null && args.length > 0) {
            // Nimbus host name passed from command line
            conf.put(Config.NIMBUS_HOST, args[0]);
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }
}
