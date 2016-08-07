package com.big.data.plan.mapreduce.join;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * reduce side join
 * map端打标签，reduce端笛卡尔积
 */
public class ReduceSideJoin {

    public static int time = 0;

    public static class Map extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)

        throws IOException, InterruptedException {

            String line = value.toString();
            String relationtype = new String();

            if (line.contains("factoryname") == true

            || line.contains("addressed") == true) {

                return;
            }

            StringTokenizer itr = new StringTokenizer(line);
            String mapkey = new String();
            String mapvalue = new String();
            int i = 0;

            while (itr.hasMoreTokens()) {

                String token = itr.nextToken();
                if (token.charAt(0) >= '0' && token.charAt(0) <= '9') {

                    mapkey = token;
                    if (i > 0) {
                        relationtype = "1";
                    } else {
                        relationtype = "2";
                    }
                    continue;
                }

                mapvalue += token + " ";
                i++;

            }

            context.write(new Text(mapkey), new Text(relationtype + "+" + mapvalue));

        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)

        throws IOException, InterruptedException {

            if (0 == time) {

                context.write(new Text("factoryname"), new Text("addressname"));
                time++;
            }

            int factorynum = 0;
            String[] factory = new String[10];
            int addressnum = 0;
            String[] address = new String[10];

            Iterator ite = values.iterator();
            while (ite.hasNext()) {

                String record = ite.next().toString();
                int len = record.length();
                int i = 2;
                if (0 == len) {
                    continue;
                }

                char relationtype = record.charAt(0);
                if ('1' == relationtype) {

                    factory[factorynum] = record.substring(i);
                    factorynum++;
                }

                if ('2' == relationtype) {

                    address[addressnum] = record.substring(i);
                    addressnum++;
                }

            }

            if (0 != factorynum && 0 != addressnum) {

                for (int m = 0; m < factorynum; m++) {

                    for (int n = 0; n < addressnum; n++) {
                        context.write(new Text(factory[m]), new Text(address[n]));
                    }

                }

            }

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {

            System.err.println("Usage: Multiple Table Join <in> <out>");

            System.exit(2);

        }

        Job job = Job.getInstance(conf, "Multiple Table Join");

        job.setJarByClass(ReduceSideJoin.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
