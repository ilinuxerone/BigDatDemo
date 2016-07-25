package hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HDFSUtils {
	public static FileSystem getFileSystem()
	{
		FileSystem hdfs = null;
		
		try {
			Configuration conf = new Configuration();
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hdfs;
	}
}
