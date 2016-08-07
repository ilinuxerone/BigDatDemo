package com.big.data.plan.mapreduce;

import java.io.IOException;

import com.big.data.plan.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hbase2Hbase {

    enum Counters {
        VALID, ROWS, COLS, ERROR
    }

    private static Logger log = LoggerFactory.getLogger(Hbase2Hbase.class);

    static class ParseMapper extends TableMapper<ImmutableBytesWritable, Put> {
        private byte[] columnFamily = null;
        private byte[] columnQualifier = null;

        @Override
        protected void setup(Mapper.Context cxt) {
            columnFamily = Bytes.toBytes(cxt.getConfiguration().get("conf.columnfamily"));
            columnQualifier = Bytes.toBytes(cxt.getConfiguration().get("conf.columnqualifier"));
        }

        @Override
        public void map(ImmutableBytesWritable row, Result columns, Mapper.Context cxt) {
            cxt.getCounter(Counters.ROWS).increment(1);
            String value = null;
            try {
                Put put = new Put(row.get());
                for (KeyValue kv : columns.list()) {
                    cxt.getCounter(Counters.COLS).increment(1);
                    value = Bytes.toStringBinary(kv.getValue());
                    if (equals(columnQualifier, kv.getQualifier())) { // ����column
                        put.add(columnFamily, columnQualifier, kv.getValue());
                        cxt.write(row, put);
                        cxt.getCounter(Counters.VALID).increment(1);
                    }
                }
            } catch (Exception e) {
                log.info("Error:" + e.getMessage() + ",Row:" + Bytes.toStringBinary(row.get())
                         + ",Value:" + value);
                cxt.getCounter(Counters.ERROR).increment(1);
            }
        }

        private boolean equals(byte[] a, byte[] b) {
            String aStr = Bytes.toString(a);
            String bStr = Bytes.toString(b);
            if (aStr.equals(bStr)) {
                return true;
            }
            return false;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException,
                                           ClassNotFoundException {
        byte[] columnFamily = Bytes.toBytes("fam");
        byte[] columnQualifier = Bytes.toBytes("data");
        Scan scan = new Scan();
        scan.addColumn(columnFamily, columnQualifier);
        HadoopUtils.initialConf();
        Configuration conf = HadoopUtils.getConf();
        conf.set("conf.columnfamily", Bytes.toStringBinary(columnFamily));
        conf.set("conf.columnqualifier", Bytes.toStringBinary(columnQualifier));

        String input = "testtable";//
        String output = "testtable1"; // 

        Job job = new Job(conf, "Parse data in " + input + ",write to" + output);
        job.setJarByClass(Hbase2Hbase.class);
        TableMapReduceUtil.initTableMapperJob(input, scan, ParseMapper.class,
            ImmutableBytesWritable.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob(output, IdentityTableReducer.class, job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
