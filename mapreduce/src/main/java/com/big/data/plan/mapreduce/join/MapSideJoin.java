package com.big.data.plan.mapreduce.join;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.big.data.plan.hdfs.HDFS_File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * map side join
 */
public class MapSideJoin extends Configured implements Tool {
    private FileSystem fs;
    private static Logger logger = Logger.getLogger(MapSideJoin.class);

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        private Map<String, String> userMap = new HashMap<String, String>();
        private Map<String, String> sexMap = new HashMap<String, String>();
        private Text oKey = new Text();

        private Text oValue = new Text();
        private String[] kv;
        private static boolean flag = true;
        private String mapKey;
        private String mapValue;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader in = null;

            try {
                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                String uidNameAddr = null;
                String sidSex = null;
                for (Path path : paths) {
                    logger.info("===========>path:" + path);
                    if (path.toString().endsWith("user.txt")) {
                        in = new BufferedReader(new FileReader(path.toString()));
                        while (null != (uidNameAddr = in.readLine())) {
                            userMap.put(uidNameAddr.split(",", -1)[0],
                                uidNameAddr.split(",", -1)[1]);
                        }
                    } else if (path.toString().endsWith("sex.txt")) {
                        in = new BufferedReader(new FileReader(path.toString()));
                        while (null != (sidSex = in.readLine())) {
                            sexMap.put(sidSex.split(",", -1)[0], sidSex.split(",", -1)[1]);
                        }
                    }
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException,
                                                                       InterruptedException {
            if (flag) {

                logger.info("==============>userMap.size():" + userMap.size());
                logger.info("==============>sexMap.size():" + sexMap.size());

                Set<Map.Entry<String, String>> userEntry = userMap.entrySet();
                for (Map.Entry<String, String> tmp : userEntry) {
                    mapKey = tmp.getKey();
                    mapValue = tmp.getValue();

                    logger.info("============>user map:" + mapKey + "," + mapValue);
                }

                Set<Map.Entry<String, String>> sexEntry = sexMap.entrySet();
                for (Map.Entry<String, String> tmp : sexEntry) {
                    mapKey = tmp.getKey();
                    mapValue = tmp.getValue();

                    logger.info("============>sex map:" + mapKey + "," + mapValue);
                }

                flag = false;
            }

            kv = value.toString().split(",");
            if (userMap.containsKey(kv[0]) && sexMap.containsKey(kv[1])) {
                oKey.set(userMap.get(kv[0]) + "\t" + sexMap.get(kv[1]));
                oValue.set("1");
                context.write(oKey, oValue);
            }
        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private Text oValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                                                                             InterruptedException {
            int sumCount = 0;
            for (Text val : values) {
                sumCount += Integer.parseInt(val.toString());
            }
            oValue.set(String.valueOf(sumCount));
            context.write(key, oValue);
        }

    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        fs = FileSystem.newInstance(conf);
        Path outPutPath = new Path(args[2]);
        fs.deleteOnExit(outPutPath);

        Path srcPath = new Path("/testsrc");
        if (!fs.exists(srcPath)) {
            fs.mkdirs(srcPath);
        }

        Path userPath = new Path(srcPath, args[0]);
        String localUserPath = new File(ClassLoader.getSystemResource("").getPath(), args[0])
            .getAbsolutePath();
        if (!fs.exists(userPath)) {
            new HDFS_File(conf).putFile(localUserPath, userPath.toString());
        }

        Path sexPath = new Path(srcPath, args[1]);
        String localSexPath = new File(ClassLoader.getSystemResource("").getPath(), args[1])
            .getAbsolutePath();
        if (!fs.exists(sexPath)) {
            new HDFS_File(conf).putFile(localSexPath, sexPath.toString());
        }

        Job job = Job.getInstance(conf, "MapSideJoin");

        job.setJobName("MapSideJoin");
        job.setJarByClass(MapSideJoin.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        DistributedCache.addCacheFile(userPath.toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(sexPath.toUri(), job.getConfiguration());

        FileInputFormat.addInputPath(job, userPath);
        FileOutputFormat.setOutputPath(job, outPutPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MapSideJoin(),
            new String[] { "user.txt", "sex.txt", "/test" });
        System.exit(res);
    }

}