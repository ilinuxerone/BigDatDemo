package com.big.data.plan;

import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Created by seven on 24/02/16.
 * 生产数据到指定目录，用于其他程序使用
 */
public class ProducerData {
    private static String[] src = { "Spark is a fast and general processing engine compatible with Hadoop data",
                                    "It can run in Hadoop clusters through YARN or Spark's standalone mode",
                                    "and it can process data in HDFS, HBase, Cassandra, Hive, and any Hadoop InputFormat",
                                    "It is designed to perform both batch processing (similar to MapReduce)",
                                    "and new workloads like streaming, interactive queries, and machine learning" };
    private static final Logger logger = Logger.getLogger(ProducerData.class);

    public void startProduce(String srcPath) {
        BufferedWriter br = null;
        File file = null;
        try {
            String fileName = Constant.PRODUCE_DATE_FORMAT.format(new Date()) + ".log";
            file = new File(srcPath, fileName);
            br = new BufferedWriter(new FileWriter(file));
            String line = null;
            for (int i = 0; i < 10000; i++) {
                Random random = new Random();
                line = src[random.nextInt(src.length)];
                br.write(line);
                br.newLine();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }

            logger.info("produce one file ok, file is " + file.getAbsolutePath());

        }
    }

    class ProducerThread implements Runnable {
        @Override
        public void run() {
            new ProducerData().startProduce("/home/seven/big-data-plan/data");
        }
    }

    public static void main(String[] args) {
        ExecutorService executorService = new ThreadPoolExecutor(10, 20, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(100));
        ProducerThread producerThread = null;
        while (true) {
            producerThread = new ProducerData().new ProducerThread();
            executorService.submit(producerThread);
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }
}
