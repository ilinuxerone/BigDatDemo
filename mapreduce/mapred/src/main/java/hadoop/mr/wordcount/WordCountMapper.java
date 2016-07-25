package hadoop.mr.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

/*	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(ivalue.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, new Text(one.toString()));
		}
	}*/
	 public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	        this.word.set(itr.nextToken());
	        context.write(this.word, one);
	      }
	 }
}
