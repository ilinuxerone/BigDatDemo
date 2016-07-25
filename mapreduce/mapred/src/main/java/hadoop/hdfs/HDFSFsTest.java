package hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSFsTest {
    public static void testRead() throws Exception{
    	Configuration conf = new Configuration();
    	
    	FileSystem hdfs = FileSystem.get(conf);    	
    	Path path = new Path("/opt/data/testRead");
    	FSDataInputStream inStream = hdfs.open(path);
    	IOUtils.copyBytes(inStream, System.out, 4096, false);
    	
    	IOUtils.closeStream(inStream);
    }
    
    public static void testList() throws FileNotFoundException, IOException
    {
    	FileSystem hdfs = HDFSUtils.getFileSystem();
    	
    	Path path = new Path("/opt/data/testList");
    	FileStatus[] fileStatus = hdfs.listStatus(path);
    	for (FileStatus fs : fileStatus)
    	{
    		Path p = fs.getPath();
    		String info = fs.isDir() ? "dir" : "file";
    		System.out.println(info + ": " + p);
    	}
    }
    
    public static void testmkdir() throws IOException  
    {
    	FileSystem hdfs = HDFSUtils.getFileSystem();
    	Path path = new Path("/opt/data/testmkdir");
    	boolean ret = hdfs.mkdirs(path);
    	System.out.println("mkdir[" + path +"]" + " " + ret);
    }
    
    public static void testPut() throws IOException 
    {
    	FileSystem hdfs = HDFSUtils.getFileSystem();
    	Path src = new Path("/opt/data/src");
    	Path dst = new Path("/opt/data/dst");
    	hdfs.copyFromLocalFile(src, dst);
    }
    
    public static void testCreate() throws IOException
    {
    	FileSystem hdfs = HDFSUtils.getFileSystem();
    	Path src = new Path("/opt/data/testCreate");
    	FSDataOutputStream fsDataOutputStream = hdfs.create(src);
    	fsDataOutputStream.writeUTF("Hello Hadoop");
    }
    
    
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
