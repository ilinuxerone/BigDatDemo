package com.big.data.plan.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Hfile2Hbase {
    public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
        String tableName = args[0];
        Path inputDir = new Path(args[1]);
        Job job = Job.getInstance(conf, "hac_chapter2_recipe3");
        job.setJarByClass(HourlyImporter.class);
        FileInputFormat.setInputPaths(job, inputDir);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(HourlyImporter.class);
        TableMapReduceUtil.initTableReducerJob(tableName, null, job);
        job.setNumReduceTasks(0);
        TableMapReduceUtil.addDependencyJars(job);
        return job;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = createSubmittableJob(conf, args);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class HourlyImporter extends
                                       Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private long ts;
        static byte[] family = Bytes.toBytes("n");

        @Override
        protected void setup(Context context) {
            ts = System.currentTimeMillis();
        }

        @Override
        public void map(LongWritable offset, Text value, Context context) throws IOException {
            try {
                String line = value.toString();
                String stationID = line.substring(0, 11);
                String month = line.substring(12, 14);
                String day = line.substring(15, 17);
                String rowkey = stationID + month + day;
                byte[] bRowKey = Bytes.toBytes(rowkey);
                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
                Put p = new Put(bRowKey);
                for (int i = 1; i < 25; i++) {
                    // String columnI = "v" + Common.lpad(String.valueOf(i), 2,
                    // '0');
                    String columnI = "v" + String.valueOf(i);
                    int beginIndex = i * 7 + 11;
                    String valueI = line.substring(beginIndex, beginIndex + 6).trim();
                    p.add(family, Bytes.toBytes(columnI), ts, Bytes.toBytes(valueI));
                }
                context.write(rowKey, p);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
