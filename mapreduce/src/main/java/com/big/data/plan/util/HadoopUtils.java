package com.big.data.plan.util;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class HadoopUtils {

	private static Configuration conf;

	public static void initialConf() {
		conf = new Configuration();
		conf.addResource("core-site.xml");
		
		conf.addResource("hbase-site.xml");
		conf.set("hbase.zookeeper.quorum", "Master.Hadoop,Salve.Hadoop");
		conf.set("hbase.zookeeper.master","Master.Hadoop");
	}

	public static Configuration getConf() {
		if (conf == null) {
			initialConf();
		}
		return conf;
	}

	public static List<String> readFromHDFS(String fileName) throws IOException {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(URI.create(fileName), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(fileName));
		// ���ж�ȡ���°汾�ķ�����
		LineReader inLine = new LineReader(hdfsInStream, conf);
		Text txtLine = new Text();

		int iResult = inLine.readLine(txtLine); // ��ȡ��һ��
		List<String> list = new ArrayList<String>();
		while (iResult > 0) {
			list.add(txtLine.toString());
			iResult = inLine.readLine(txtLine);
		}

		hdfsInStream.close();
		fs.close();
		return list;
	}
}
