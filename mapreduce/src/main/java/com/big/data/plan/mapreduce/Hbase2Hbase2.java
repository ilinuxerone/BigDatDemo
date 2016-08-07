package com.big.data.plan.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.big.data.plan.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hbase2Hbase2 {

    enum Counters {
        VALID, ROWS, COLS, ERROR
    }

    private static Logger log = LoggerFactory.getLogger(ParseMapper.class);

    static class ParseMapper extends TableMapper<ImmutableBytesWritable, Writable> {
        private HTable infoTable = null;
        private byte[] columnFamily = null;
        private byte[] columnQualifier = null;
        private byte[] columnQualifier1 = null;
        private List<Put> list = new ArrayList<Put>();

        @Override
        protected void setup(Mapper.Context cxt) {
            log.info("ParseListPutDriver setup,current time: " + new Date());
            try {
                infoTable = new HTable(cxt.getConfiguration(),
                    cxt.getConfiguration().get("conf.infotable"));
                infoTable.setAutoFlush(false);
            } catch (IOException e) {
                log.error("Initial infoTable error:\n" + e.getMessage());
            }
            columnFamily = Bytes.toBytes(cxt.getConfiguration().get("conf.columnfamily"));
            columnQualifier = Bytes.toBytes(cxt.getConfiguration().get("conf.columnqualifier"));
            columnQualifier1 = Bytes.toBytes(cxt.getConfiguration().get("conf.columnqualifier1"));
        }

        @Override
        protected void cleanup(Mapper.Context cxt) {
            try {
                infoTable.put(list);
                infoTable.flushCommits();
                log.info("ParseListPutDriver cleanup ,current time :" + new Date());
            } catch (IOException e) {
                log.error("infoTable flush commits error:\n" + e.getMessage());
            }
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
                        put.add(columnFamily, columnQualifier1, kv.getValue());
                        list.add(put);
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
        String input = "testtable";
        byte[] columnFamily = Bytes.toBytes("fam");
        byte[] columnQualifier = Bytes.toBytes("data");
        byte[] columnQualifier1 = Bytes.toBytes("data2");
        Scan scan = new Scan();
        scan.addColumn(columnFamily, columnQualifier);
        HadoopUtils.initialConf();
        Configuration conf = HadoopUtils.getConf();
        conf.set("conf.columnfamily", Bytes.toStringBinary(columnFamily));
        conf.set("conf.columnqualifier", Bytes.toStringBinary(columnQualifier));
        conf.set("conf.columnqualifier1", Bytes.toStringBinary(columnQualifier1));
        conf.set("conf.infotable", input);

        Job job = new Job(conf, "Parse data in " + input + ",into tables");
        job.setJarByClass(Hbase2Hbase2.class);
        TableMapReduceUtil.initTableMapperJob(input, scan, ParseMapper.class,
            ImmutableBytesWritable.class, Put.class, job);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}