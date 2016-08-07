package com.big.data.plan.mapreduce.secondsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 联合key
 * sort
 * partition
 * group sort
 */
public class SecondarySort {
    public static class IntPair implements WritableComparable<IntPair> {
        int first;
        int second;

        /**
         * Set the left and right values.
         */
        public void set(int left, int right) {
            first = left;
            second = right;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        @Override
        public void readFields(DataInput in) throws IOException {

            first = in.readInt();
            second = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {

            out.writeInt(first);
            out.writeInt(second);
        }

        @Override
        public int compareTo(IntPair o) {

            if (first != o.first) {
                return first < o.first ? -1 : 1;
            } else if (second != o.second) {
                return second < o.second ? -1 : 1;
            } else {
                return 0;
            }
        }

        @Override
        // The hashCode() method is used by the HashPartitioner (the default
        // partitioner in MapReduce)
        public int hashCode() {
            return first * 157 + second;
        }

        @Override
        public boolean equals(Object right) {
            if (right == null)
                return false;
            if (this == right)
                return true;
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
                return false;
            }
        }
    }

    public static class FirstPartitioner extends Partitioner<IntPair, IntWritable> {
        @Override
        public int getPartition(IntPair key, IntWritable value, int numPartitions) {
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(IntPair.class, true);
        }

        @Override
        // Compare two WritableComparables.
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            int l = ip1.getFirst();
            int r = ip2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }

    public static class Map extends Mapper<LongWritable, Text, IntPair, IntWritable> {
        private final IntPair intkey = new IntPair();
        private final IntWritable intvalue = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException,
                                                                       InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            int left = 0;
            int right = 0;
            if (tokenizer.hasMoreTokens()) {
                left = Integer.parseInt(tokenizer.nextToken());
                if (tokenizer.hasMoreTokens())
                    right = Integer.parseInt(tokenizer.nextToken());
                intkey.set(left, right);
                intvalue.set(right);
                context.write(intkey, intvalue);
            }
        }
    }

    public static class Reduce extends Reducer<IntPair, IntWritable, Text, IntWritable> {
        private final Text left = new Text();
        private static final Text SEPARATOR = new Text(
            "------------------------------------------------");

        public void reduce(IntPair key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            context.write(SEPARATOR, null);
            left.set(Integer.toString(key.getFirst()));
            for (IntWritable val : values) {
                context.write(left, val);
            }
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException, InterruptedException,
                                           ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "secondarysort");
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}